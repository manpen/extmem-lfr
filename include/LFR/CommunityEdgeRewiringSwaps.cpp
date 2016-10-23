#include <LFR/CommunityEdgeRewiringSwaps.h>
#include <EdgeSwaps/EdgeSwapInternalSwapsBase_impl.h>
#include <Utils/RandomBoolStream.h>

#include <Utils/RandomSeed.h>

//#define EXIT_AFTER_COM_REWIRING

void CommunityEdgeRewiringSwaps::run() {
    std::mt19937_64 gen(RandomSeed::get_instance().get_next_seed());
    std::minstd_rand fast_gen(RandomSeed::get_instance().get_next_seed());
    RandomBoolStream _bool_stream(RandomSeed::get_instance().get_next_seed());

    size_t last_edges_found = std::numeric_limits<size_t>::max();
    unsigned int retry_count = 0;

    unsigned int iterations = 0;

    const bool addition_random = _random_edge_ratio > std::numeric_limits<double>::epsilon();

    while (true) {
        ++iterations;

        // generate vector of struct { community, duplicate edge, partner edge }.
        std::vector<community_swap_edges_t> com_swap_edges;

        std::vector<edgeid_t> _community_sizes; // size of each community in terms of edges
        std::vector<bool> duplicate_exists_in_community;
        community_t no_real_communities_with_duplicates = 0;

        auto countCommunity = [&](community_t c) {
            if (static_cast<community_t>(_community_sizes.size()) <= c) {
                _community_sizes.resize(c + 1);
            }
            ++_community_sizes[c];
        };

        auto markDuplicate = [&](community_t c) {
            if (static_cast<community_t>(duplicate_exists_in_community.size()) <= c)
                duplicate_exists_in_community.resize(c+1, false);

            no_real_communities_with_duplicates += !duplicate_exists_in_community[c];
            duplicate_exists_in_community[c] = true;
        };

        const auto no_edges = _community_edges.size();

        { // first pass: load at most max_swaps duplicate edges duplicate edge in com_swap_edges
            edge_community_t last_edge = {0, {-1, -1}};
            bool inDuplicates = false;
            int_t first_dup = 0;
            loadAndStoreEdges([&](edgeid_t eid, const edge_community_t &e) {
                assert(!e.edge.is_loop());
                countCommunity(e.community_id);

                if (UNLIKELY(last_edge.edge == e.edge && (inDuplicates || com_swap_edges.size() < _max_swaps))) {
                    if (!LIKELY(inDuplicates)) {
                        first_dup = com_swap_edges.size();
                        com_swap_edges.push_back(community_swap_edges_t {last_edge.community_id, last_edge.edge, edge_t::invalid()});
                        _edge_ids_in_current_swaps.push_back(eid - 1);
                        markDuplicate(last_edge.community_id);
                        inDuplicates = true;
                    }

                    com_swap_edges.push_back(community_swap_edges_t {e.community_id, e.edge, edge_t::invalid()});
                    _edge_ids_in_current_swaps.push_back(eid);
                    markDuplicate(e.community_id);
                    assert(com_swap_edges.size() == _edge_ids_in_current_swaps.size());

                } else if (UNLIKELY(inDuplicates)) {
                    do {
                        std::uniform_int_distribution<int_t> dis(first_dup, com_swap_edges.size() - 1);
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
        if (duplicate_exists_in_community.size() < static_cast<size_t>(numCommunities))
            duplicate_exists_in_community.resize(numCommunities, false);

        assert(std::accumulate(duplicate_exists_in_community.begin(), duplicate_exists_in_community.end(), community_t(0)) == no_real_communities_with_duplicates);


        std::cout << "---- Rewiring Iteration: " << iterations
                  << " duplicates: " << com_swap_edges.size()
                  << " edges: " << no_edges
                  << " fraction: " << (100. * com_swap_edges.size() / no_edges)
                  << std::endl;


        // no duplicates found - nothing to do anymore!
        if (com_swap_edges.empty())
            break;

#ifdef EXIT_AFTER_COM_REWIRING
        if (iterations == 200)
            break;
#else
        if (com_swap_edges.size() == last_edges_found && last_edges_found < no_edges * 1e-3) {
            if (++retry_count == 5) {
                std::cout << "CommunityEdgeRewiringSwaps does not converge; give up and delete duplicates" << std::endl;
                deleteDuplicates();
                break;
            }
        } else {
            retry_count = 0;
            last_edges_found = com_swap_edges.size();
        }
#endif


        // bucket sort of com_swap_edges
        std::vector<edgeid_t> duplicates_per_community(_community_sizes.size() + 1);

        for (const community_swap_edges_t &e : com_swap_edges) {
            ++duplicates_per_community[e.community_id];
        }
        assert(!duplicates_per_community.back());


        // exclusive prefix sum
        uint_t sum = 0;
        community_t no_comm_with_duplicates = 0;
        for (uint_t i = 0; i < duplicates_per_community.size(); ++i) {
            edgeid_t tmp = duplicates_per_community[i];
            no_comm_with_duplicates += bool(tmp);

            duplicates_per_community[i] = sum;
            sum += tmp;
        }
        assert(no_comm_with_duplicates <= no_real_communities_with_duplicates);

        // sort in buckets
        {
            std::vector<community_swap_edges_t> tmp(com_swap_edges.size());

            for (const auto &x : com_swap_edges) {
                tmp[duplicates_per_community[x.community_id]++] = x;
            }

            com_swap_edges = std::move(tmp);
        }

        // reset index to original state
        {
            edgeid_t tmp = 0;
            for (uint_t i = 0; i < duplicates_per_community.size(); ++i) {
                std::swap(duplicates_per_community[i], tmp);
            }
        }

        assert(com_swap_edges.size() == static_cast<size_t>(duplicates_per_community.back()));

        std::vector<edgeid_t> swap_partner_id_per_community(com_swap_edges.size());

        {
            for (uint_t i = 0; i < com_swap_edges.size(); ++i) {
                std::uniform_int_distribution<> dis(0, _community_sizes[com_swap_edges[i].community_id] - 1);
                swap_partner_id_per_community[i] = dis(gen);
            }
        }


        // compute number of random swaps to perform
        std::vector<edgeid_t> random_swaps_per_community(_community_sizes.size() + 1);
        random_swaps_per_community[0] = 0;
        community_t no_comm_with_random = 0;

        {
            const edgeid_t dups = duplicates_per_community.back();
            edgeid_t rand_swaps_left = static_cast<edgeid_t>(_max_swaps) > dups ? (_max_swaps - dups) : 0;

            const bool end_game_random =
                    (no_comm_with_duplicates < numCommunities * 1e-2) ||
                    (duplicates_per_community.back() < no_edges * 2e-2);

            for (community_t com = 0; com < numCommunities; ++com) {
                std::cout << end_game_random << " "
                          << com << " "
                          << duplicate_exists_in_community[com] << " "
                          << no_real_communities_with_duplicates << " "
                          << no_comm_with_random << " "
                          << rand_swaps_left << " "
                          << std::endl;


                edgeid_t swaps = 0;
                const auto dups_in_com = duplicates_per_community[com + 1] - duplicates_per_community[com];

                assert(!dups_in_com || duplicate_exists_in_community[com]);

                if (addition_random && duplicate_exists_in_community[com]) {
                    if (end_game_random) {
                        assert(no_comm_with_random < no_real_communities_with_duplicates);
                        swaps = static_cast<edgeid_t>(rand_swaps_left / (no_real_communities_with_duplicates - no_comm_with_random));

                    } else {
                        swaps = dups_in_com * _random_edge_ratio;

                    }

                    swaps = std::min<edgeid_t>(swaps, _community_sizes[com] / 2);
                    swaps = std::min<edgeid_t>(swaps, rand_swaps_left);

                    rand_swaps_left -= swaps;

                    std::cout << swaps << std::endl;

                    no_comm_with_random += bool(swaps);


                }

                random_swaps_per_community[com + 1] = random_swaps_per_community[com] + swaps;
            }
        }

        std::cout << iterations << "   "

                  << duplicates_per_community.back() << " "
                  << random_swaps_per_community.back() << "   "

                  << no_comm_with_duplicates << " "
                  << no_real_communities_with_duplicates << " "
                  << no_comm_with_random << "   "

                  << numCommunities << " "
                  << no_edges

                  << "# ComRewStats iter, #dups, #rand-swps, comms-with-dup, comms-with-real-dup, comms-with-rand, #comms, #edges"
                  << std::endl;


        std::vector<random_edge_t> random_edges(random_swaps_per_community.back() * 2);

        // sort swap partners per community in decreasing order
        // and shuffle swaps of same community.
        const auto tmp_seed = RandomSeed::get_instance().get_next_seed();
        #pragma omp parallel
        {
            #pragma omp for schedule(guided)
            for (community_t com = 0; com < numCommunities; ++com) {
                std::minstd_rand fast_gen(RandomSeed::get_instance().get_seed(tmp_seed + com));
                std::sort(swap_partner_id_per_community.begin() + duplicates_per_community[com],
                          swap_partner_id_per_community.begin() + duplicates_per_community[com + 1], std::greater<edgeid_t>());
                std::shuffle(com_swap_edges.begin() + duplicates_per_community[com], com_swap_edges.begin() + duplicates_per_community[com + 1], fast_gen);
            }
        }

        {
            std::vector<edgeid_t> next_edge_per_community(duplicates_per_community);
            std::vector<edgeid_t> edges_sampled_for_community(numCommunities, 0);

            // Load swap partners in second item in order without shuffling.
            // second pass: load all swap partners by scanning over all edges, decrementing community sizes and storing an edge whenever the next partner of the current community matches the remaining size
            {
                stxxl::vector<edge_community_t>::bufreader_type community_edge_reader(_community_edges);
                for (edgeid_t eid = 0; !community_edge_reader.empty(); ++community_edge_reader, ++eid) {
                    community_t com = community_edge_reader->community_id;
                    assert(com < numCommunities);
                    --_community_sizes[com];

                    while (next_edge_per_community[com] < duplicates_per_community[com + 1]
                           && swap_partner_id_per_community[next_edge_per_community[com]] == _community_sizes[com]) {

                        com_swap_edges[next_edge_per_community[com]].partner_edge = community_edge_reader->edge;
                        _edge_ids_in_current_swaps.push_back(eid);
                        ++next_edge_per_community[com];
                    }

                    // todo: check how expensive reservoir sampling is compared to drawing and sorting of ids
                    const auto edges = 2 * (random_swaps_per_community[com + 1] - random_swaps_per_community[com]);
                    if (UNLIKELY(edges_sampled_for_community[com] < edges)) {
                        random_edges.at(2 * random_swaps_per_community[com] + edges_sampled_for_community[com])
                                = {eid, community_edge_reader->edge, com};

                    } else if (edges) {
                        std::uniform_int_distribution<edgeid_t> dis(0, edges_sampled_for_community[com]);
                        const auto r = dis(gen);

                        if (UNLIKELY(r < edges))
                            random_edges.at(2 * random_swaps_per_community[com] + r) = {eid, community_edge_reader->edge, com};

                    }

                    ++edges_sampled_for_community[com];
                }
            }

#ifndef NDEBUG
            for (community_t com = 0; com < numCommunities; ++com)
                assert(edges_sampled_for_community[com] >= 2 * (random_swaps_per_community[com + 1] - random_swaps_per_community[com]));
#endif
        }

        // Shuffle all swaps.
        std::shuffle(com_swap_edges.begin(), com_swap_edges.end(), fast_gen);

        // generate vector of real swaps with internal ids and internal edge vector.
        swapid_t num_swaps = com_swap_edges.size() + random_swaps_per_community.back();
        _current_swaps.clear();
        _current_swaps.reserve(num_swaps);
        _edges_in_current_swaps.clear();
        _edges_in_current_swaps.reserve(num_swaps * 2);
        _swap_has_successor[0].clear();
        _swap_has_successor[0].resize(num_swaps, false);
        _swap_has_successor[1].clear();
        _swap_has_successor[1].resize(num_swaps, false);

        //fill them by sorting (edge, swap_id, swap_pos) pairs and identifying duplicates.
        std::vector<edge_community_swap_t> swapRequests;
        swapRequests.reserve(num_swaps * 2);
        for (uint_t i = 0; i < com_swap_edges.size(); ++i) {
            swapRequests.push_back(edge_community_swap_t {com_swap_edges[i].duplicate_edge, com_swap_edges[i].community_id, i, 0});
            swapRequests.push_back(edge_community_swap_t {com_swap_edges[i].partner_edge, com_swap_edges[i].community_id, i, 1});

            // Generate swap with random direction (and we must set edge ids that are not equal...)
            _current_swaps.push_back(SwapDescriptor(0, 1, *_bool_stream));
            ++_bool_stream;
        }

        // generate swap descriptors from random swaps
        {
            auto it = random_edges.begin();
            swapid_t sid = _current_swaps.size();
            for (community_t com = 0; com < numCommunities; ++com) {
                for (edgeid_t i = random_swaps_per_community[com]; i < random_swaps_per_community[com + 1]; i++) {
                    for (unsigned char p = 0; p < 2; ++p, ++it) {
                        assert(it != random_edges.end());
                        assert(!it->edge.is_invalid());
                        assert(it->comm == com);

                        _edge_ids_in_current_swaps.push_back(it->id);
                        swapRequests.push_back(edge_community_swap_t{it->edge, com, sid, p});
                    }

                    _current_swaps.push_back(SwapDescriptor(1, 2, *_bool_stream));
                    ++sid;
                    ++_bool_stream;
                }
            }
            assert(it == random_edges.end());
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
                _community_of_current_edge.push_back(sr.community_id);
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

    std::cout << "[CommunityEdgeRewiringSwaps] Number of iterations: " << (iterations-1) << std::endl;

#ifdef EXIT_AFTER_COM_REWIRING
    std::cout << "Maximum EM allocation: " <<  stxxl::block_manager::get_instance()->get_maximum_allocation() << std::endl;
    exit(0);
#endif
}

// fallback, if we dont converge -- then just delete duplicates
void CommunityEdgeRewiringSwaps::deleteDuplicates() {
    if (UNLIKELY(_community_edges.empty()))
        return;

    // read old edge vector and merge in updates, write out result
    stxxl::vector<edge_community_t>::bufreader_type community_edge_reader(_community_edges);

    edge_community_vector_t output_vector;
    output_vector.reserve(_community_edges.size());
    edge_community_vector_t::bufwriter_type writer(output_vector);

    STDRandomEngine gen(RandomSeed::get_instance().get_next_seed());

    edge_community_t last_edge = *community_edge_reader;
    edgeid_t last_edge_occ = 0;
    edgeid_t edges_written = 1;

    for(++community_edge_reader; !community_edge_reader.empty(); ++community_edge_reader) {
        const auto & edge = *community_edge_reader;

        if (LIKELY(last_edge.edge != edge.edge)) {
            if (LIKELY(!edge.edge.is_invalid())) {
                writer << last_edge;
                edges_written++;
            }

            last_edge = edge;
            last_edge_occ = 1;

        } else {
            std::uniform_int_distribution<edgeid_t> distr(0, last_edge_occ);
            last_edge_occ++;
            if (!distr(gen)) last_edge.community_id = edge.community_id;
        }
    }

    writer << last_edge;
    writer.finish();

    std::cout << "Wrote " << edges_written << " edges, i.e. deleted " << (_community_edges.size() - edges_written) << std::endl;

    _community_edges.swap(output_vector);
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
        assert(!i || updated_edges[i-1] != updated_edges[i]);
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
                while (UNLIKELY(old_e != old_edge_ids.end() && *old_e == read_id)) {
                    ++community_edge_reader;
                    ++read_id;
                    ++old_e;
                }

                // merge update edges and read edges
                if (UNLIKELY(new_e != updated_edges.end() && (community_edge_reader.empty() || *new_e < *community_edge_reader))) {
                    assert(*new_e != cur_e);
                    cur_e = *new_e;

                    writer << cur_e;
                    ++new_e;

                } else {
                    if (UNLIKELY(community_edge_reader.empty())) { // due to the previous while loop both could be empty now
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
