#include <LFR/CommunityEdgeRewiringSwaps.h>
#include <EdgeSwaps/EdgeSwapInternalSwapsBase_impl.h>
#include <Utils/RandomBoolStream.h>

void CommunityEdgeRewiringSwaps::run() {
    while (true) {
        // generate vector of struct { community, duplicate edge, partner edge }.
        std::vector<community_swap_edges_t> com_swap_edges;

        std::vector<int_t> _community_sizes; // size of each community in terms of edges
        auto countCommunity = [&](community_t c) {
            if (static_cast<community_t>(_community_sizes.size()) <= c) {
                _community_sizes.resize(c + 1);
            }
            ++_community_sizes[c];
        };

        std::mt19937 gen(stxxl::get_next_seed());

        { // first pass: load at most max_swaps duplicate edges duplicate edge in com_swap_edges
            edge_community_t last_edge = {0, {-1, -1}};
            bool inDuplicates = false;
            int_t first_dup = 0;
            loadAndStoreEdges([&](edgeid_t eid, const edge_community_t &e) {
                assert(!e.edge.is_loop());
                countCommunity(e.community_id);

                if (last_edge.edge == e.edge && (inDuplicates || com_swap_edges.size() < _max_swaps)) {
                    if (!inDuplicates) {
                        first_dup = com_swap_edges.size();
                        com_swap_edges.push_back(community_swap_edges_t {last_edge.community_id, last_edge.edge, edge_t::invalid()});
                        _edge_ids_in_current_swaps.push_back(eid-1);
                        inDuplicates = true;
                    }

                    com_swap_edges.push_back(community_swap_edges_t {e.community_id, e.edge, edge_t::invalid()});
                    _edge_ids_in_current_swaps.push_back(eid);
                    assert(com_swap_edges.size() == _edge_ids_in_current_swaps.size());
                } else if (inDuplicates) {
                    do {
                        std::uniform_int_distribution<> dis(first_dup, com_swap_edges.size()-1);
                        int_t x = dis(gen);

                        assert(com_swap_edges.size() == _edge_ids_in_current_swaps.size());
                        // remove one duplicate uniformly at random
                        std::swap(com_swap_edges[x], com_swap_edges.back());
                        std::swap(_edge_ids_in_current_swaps[x], _edge_ids_in_current_swaps.back());
                        com_swap_edges.pop_back();
                        _edge_ids_in_current_swaps.pop_back();
                    } while (com_swap_edges.size() >= _max_swaps);

                    inDuplicates = false;
                }

                last_edge = e;
            });
        }

        community_t numCommunities = _community_sizes.size();

        STXXL_MSG("Found " << com_swap_edges.size() << " duplicates which shall be rewired");

        // no duplicates found - nothing to do anymore!
        // FIXME introduce threshold
        if (com_swap_edges.empty()) return;

        // bucket sort of com_swap_edges
        std::vector<uint_t> duplicates_per_community(_community_sizes.size() + 1);

        for (const community_swap_edges_t &e : com_swap_edges) {
            ++duplicates_per_community[e.community_id];
        }

        // exclusive prefix sum
        uint_t sum = 0;
        for (uint_t i = 0; i < duplicates_per_community.size(); ++i) {
            uint_t tmp = duplicates_per_community[i];
            duplicates_per_community[i] = sum;
            sum += tmp;
        }

        // sort in buckets
        {
            std::vector<community_swap_edges_t> tmp(com_swap_edges.size());

            for (const auto &x : com_swap_edges) {
                tmp[duplicates_per_community[x.community_id]++] = x;
            }

            com_swap_edges = std::move(tmp);
        }

        // reset index to original state
        for (uint_t i = 0, tmp = 0; i < duplicates_per_community.size(); ++i) {
            std::swap(duplicates_per_community[i], tmp);
        }

        assert(com_swap_edges.size() == duplicates_per_community.back());

        std::vector<edgeid_t> swap_partner_id_per_community(com_swap_edges.size());

        {
            for (uint_t i = 0; i < com_swap_edges.size(); ++i) {
                std::uniform_int_distribution<> dis(0, _community_sizes[com_swap_edges[i].community_id]-1);
                swap_partner_id_per_community[i] = dis(gen);
            }
        }


        // sort swap partners per community in decreasing order
        // and shuffle swaps of same community.
        #pragma omp parallel
        {

            std::random_device lrd;
            std::minstd_rand fast_gen(lrd());

            #pragma omp for schedule(guided)
            for (community_t com = 0; com < numCommunities; ++com) {
                std::sort(swap_partner_id_per_community.begin() + duplicates_per_community[com], swap_partner_id_per_community.begin() + duplicates_per_community[com+1], std::greater<edgeid_t>());
                std::shuffle(com_swap_edges.begin() + duplicates_per_community[com], com_swap_edges.begin() + duplicates_per_community[com+1], fast_gen);
            }
        }

        {
            std::vector<uint_t> next_edge_per_community(duplicates_per_community);

            // Load swap partners in second item in order without shuffling.
            // second pass: load all swap partners by scanning over all edges, decrementing community sizes and storing an edge whenever the next partner of the current community matches the remaining size
            {
                stxxl::vector<edge_community_t>::bufreader_type community_edge_reader(_community_edges);
                for (edgeid_t eid = 0; !community_edge_reader.empty(); ++community_edge_reader, ++eid) {
                    community_t com = community_edge_reader->community_id;
                    --_community_sizes[com];

                    while (next_edge_per_community[com] < duplicates_per_community[com+1] && swap_partner_id_per_community[next_edge_per_community[com]] == _community_sizes[com]) {
                        com_swap_edges[next_edge_per_community[com]].partner_edge = community_edge_reader->edge;
                        _edge_ids_in_current_swaps.push_back(eid);
                        ++next_edge_per_community[com];
                    }
                }
            }
        }


        // Shuffle all swaps.
        std::minstd_rand fast_gen(stxxl::get_next_seed());
        std::shuffle(com_swap_edges.begin(), com_swap_edges.end(), fast_gen);

        // generate vector of real swaps with internal ids and internal edge vector.
        _current_swaps.clear();
        _current_swaps.reserve(com_swap_edges.size());
        RandomBoolStream _bool_stream;
        _edges_in_current_swaps.clear();
        _edges_in_current_swaps.reserve(com_swap_edges.size() * 2);
        _swap_has_successor[0].clear();
        _swap_has_successor[0].resize(com_swap_edges.size(), false);
        _swap_has_successor[1].clear();
        _swap_has_successor[1].resize(com_swap_edges.size(), false);

        //fill them by sorting (edge, swap_id, swap_pos) pairs and identifying duplicates.
        std::vector<edge_community_swap_t> swapRequests;
        swapRequests.reserve(com_swap_edges.size() * 2);
        for (uint_t i = 0; i < com_swap_edges.size(); ++i) {
            swapRequests.push_back(edge_community_swap_t {com_swap_edges[i].duplicate_edge,com_swap_edges[i].community_id, i, 0});
            swapRequests.push_back(edge_community_swap_t {com_swap_edges[i].partner_edge, com_swap_edges[i].community_id, i, 1});
            // Generate swap with random direction (and we must set edge ids that are not equal...)
            _current_swaps.push_back(SwapDescriptor(0, 1, *_bool_stream));
            ++_bool_stream;
        }

        SEQPAR::sort(swapRequests.begin(), swapRequests.end());

        edgeid_t int_eid = -1;
        edge_t last_e = {-1, -1};
        uint_t last_sid = 0;
        community_t last_com = -1;
        unsigned char last_spos = 0;
        for (const auto &sr : swapRequests) {
            if (sr.e == last_e && sr.community_id == last_com) {
                _swap_has_successor[last_spos][last_sid] = true;
            } else {
                _edges_in_current_swaps.push_back(sr.e);
                _community_of_current_edge.push_back(com_swap_edges[sr.sid].community_id);
                ++int_eid;
            }

            assert(static_cast<uint_t>(int_eid) == _edges_in_current_swaps.size() - 1);
            _current_swaps[sr.sid].edges()[sr.spos] = int_eid;

            last_e = sr.e;
            last_spos = sr.spos;
            last_sid = sr.sid;
            last_com = sr.community_id;
        }

        // make edge ids unique (it might be that we selected the same edge twice...)
        SEQPAR::sort(_edge_ids_in_current_swaps.begin(), _edge_ids_in_current_swaps.end());
        _edge_ids_in_current_swaps.erase(std::unique(_edge_ids_in_current_swaps.begin(), _edge_ids_in_current_swaps.end()), _edge_ids_in_current_swaps.end());

        assert(_edge_ids_in_current_swaps.size() == _edges_in_current_swaps.size());

        {
            EdgeReaderWrapper edgeReader(_community_edges);
            executeSwaps(_current_swaps, _edges_in_current_swaps, _swap_has_successor, edgeReader);
        }

    }
}

template <typename Callback>
void CommunityEdgeRewiringSwaps::loadAndStoreEdges(Callback callback) {
    // copy old edge ids for writing back
    std::vector<edgeid_t> old_edge_ids;
    old_edge_ids.swap(_edge_ids_in_current_swaps);

    // copy updated edges for writing back
    std::vector<edge_community_t> updated_edges;
    updated_edges.reserve(_edges_in_current_swaps.size());
    for (size_t i = 0; i < _edges_in_current_swaps.size(); ++i) {
        updated_edges.emplace_back(_community_of_current_edge[i], _edges_in_current_swaps[i]);
    }

    assert(old_edge_ids.size() == updated_edges.size());

    SEQPAR::sort(updated_edges.begin(), updated_edges.end());

    _edges_in_current_swaps.clear();
    _community_of_current_edge.clear();

    {
        edgeid_t id = 0;

        stxxl::vector<edge_community_t>::bufreader_type community_edge_reader(_community_edges);
        if (updated_edges.empty()) {
            // just read edges
            for (; !community_edge_reader.empty(); ++id) {
                callback(id, *community_edge_reader);
#ifndef NDEBUG
                auto previous = *community_edge_reader;
#endif
                ++community_edge_reader;
#ifndef NDEBUG
                if (!community_edge_reader.empty() && previous == *community_edge_reader) {
                    std::cout << previous << " equals " << *community_edge_reader << std::endl;
                    assert(false && "Previous must not be the same as the next edge!");
                }
#endif
            }

        } else {
            // read old edge vector and merge in updates, write out result
            edge_community_vector_t output_vector;
            output_vector.reserve(_community_edges.size());
            edge_community_vector_t::bufwriter_type writer(output_vector);

            auto old_e = old_edge_ids.begin();
            auto new_e = updated_edges.begin();

            int_t read_id = 0;
            edge_community_t cur_e;

            for (; !community_edge_reader.empty() || new_e != updated_edges.end(); ++id) {
                // Skip old edges
                while (old_e != old_edge_ids.end() && *old_e == read_id) {
                    ++community_edge_reader;
                    ++read_id;
                    ++old_e;
                }

                // merge update edges and read edges
                if (new_e != updated_edges.end() && (community_edge_reader.empty() || *new_e < *community_edge_reader)) {
                    assert(*new_e != cur_e);
                    cur_e = *new_e;
                    writer << cur_e;
                    ++new_e;
                } else {
                    if (community_edge_reader.empty()) { // due to the previous while loop both could be empty now
                        break; // abort the loop as we do not have any edges to process anymore.
                    }

                    assert(cur_e != *community_edge_reader);
                    cur_e = *community_edge_reader;
                    writer << cur_e;
                    ++read_id;
                    ++community_edge_reader;
                }

                assert(static_cast<size_t>(id) < _community_edges.size());

                callback(id, cur_e);
            }

            writer.finish();
            assert(_community_edges.size() == output_vector.size());
            _community_edges.swap(output_vector);
        }
    }
}
