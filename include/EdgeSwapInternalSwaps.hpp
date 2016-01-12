#pragma once

#include <stxxl/vector>
#include <stxxl/sorter>
#include <tuple>
#include <set>
#include <vector>
#include <stack>
#include <functional>
#include "EdgeSwapBase.h"
#include "GenericComparator.h"
#include <TupleHelper.h>
#include <algorithm>
#include <iterator>

template <class EdgeVector = stxxl::vector<edge_t>, class SwapVector = stxxl::vector<SwapDescriptor>>
class EdgeSwapInternalSwaps : public EdgeSwapBase {
public:
    using debug_vector = stxxl::vector<SwapResult>;
    using edge_vector = EdgeVector;
    using swap_vector = SwapVector;
    using swap_descriptor = typename swap_vector::value_type;

protected:
    EdgeVector & _edges;
    SwapVector & _swaps;

    debug_vector _results;

    int_t _num_swaps_per_iteration;
    std::vector<swap_descriptor> _current_swaps;
    std::vector<edgeid_t> _edge_ids_in_current_swaps;
    std::vector<edge_t> _edges_in_current_swaps;

    std::vector<bool> _swap_has_successor[2];

    struct edge_existence_request_t {
        edge_t e;
        int_t sid;
        bool forward_only; // if this requests is only for generating the correct forwaring information but no existence information is needed
        DECL_TO_TUPLE(e, sid, forward_only);
        const auto to_compare_tuple() const -> decltype(std::make_tuple(e, sid, forward_only)) {
            return std::make_tuple(e, std::numeric_limits<int_t>::max() - sid, forward_only);
        }
        bool operator< (const edge_existence_request_t& o) const {return to_compare_tuple() <  o.to_compare_tuple(); }
    };

    stxxl::sorter<edge_existence_request_t, typename GenericComparatorStruct<edge_existence_request_t>::Ascending> _query_sorter; // Query of possible conflict edges. This may be large (too large...)

    struct edge_existence_answer_t {
        int_t sid;
        edge_t e;
#ifndef NDEBUG
        bool exists;
#endif
        DECL_LEX_COMPARE(edge_existence_answer_t, sid, e);
    };

    std::vector<edge_existence_answer_t> _edge_existence_pq;

    struct edge_existence_successor_t {
        int_t from_sid;
        edge_t e;
        int_t to_sid;
        DECL_LEX_COMPARE(edge_existence_successor_t, from_sid, e);
    };

    std::vector<edge_existence_successor_t> _edge_existence_successors;

    void simulateSwapsAndGenerateEdgeExistenceQuery() {
        // stores for a swap and the position in the swap (0,1) the edge
        struct swap_edge_t {
            int_t sid;
            unsigned char spos;
            edge_t e;

            DECL_LEX_COMPARE(swap_edge_t, sid, spos, e);
        };


        { // find possible conflicts
            // construct possible conflict pairs
            std::vector<std::vector<edge_t>> possibleEdges(_edges_in_current_swaps.size());
            std::vector<edge_t> current_edges[2];
            std::vector<edge_t> new_edges[2];

            for (auto s_it = _current_swaps.begin(); s_it != _current_swaps.end(); ++s_it) {
                const auto sid = (s_it - _current_swaps.begin());
                const auto& eids = s_it->edges();


                for (unsigned char spos = 0; spos < 2; ++spos) {
                    current_edges[spos].clear();
                    new_edges[spos].clear();
                    if (! possibleEdges[eids[spos]].empty()) {
                        current_edges[spos].swap(possibleEdges[eids[spos]]);
                    }

                    current_edges[spos].push_back(_edges_in_current_swaps[eids[spos]]);

                    for (const auto &e : current_edges[spos]) {
                        _query_sorter.push(edge_existence_request_t {e, sid, true});
                    }
                }

                assert(!current_edges[0].empty());
                assert(!current_edges[1].empty());

                // Iterate over all pairs of edges and try the swap
                for (const auto &e0 : current_edges[0]) {
                    for (const auto &e1 : current_edges[1]) {
                        edge_t t[2];
                        std::tie(t[0], t[1]) = _swap_edges(e0, e1, s_it->direction());

                        // record the two conflict edges unless the conflict is trivial
                        if (t[0].first != t[0].second && t[1].first != t[1].second) {
                            for (unsigned char spos = 0; spos < 2; ++spos) {
                                if (_swap_has_successor[spos][sid]) { // forward the new candidates for our two edge ids to possible successors
                                    new_edges[spos].push_back(t[spos]);
                                }

                                // record the query
                                _query_sorter.push(edge_existence_request_t {t[spos], sid, false});
                            }
                        }
                    }
                }

                for (unsigned char spos = 0; spos < 2; ++spos) {
                    if (_swap_has_successor[spos][sid]) {
                        current_edges[spos].pop_back(); // remove the added original edge as it will be loaded again anyway!
                        std::sort(new_edges[spos].begin(), new_edges[spos].end());
                        auto last = std::unique(new_edges[spos].begin(), new_edges[spos].end());
                        new_edges[spos].erase(last, new_edges[spos].end());
                        std::set_union(current_edges[spos].begin(), current_edges[spos].end(),
                                       new_edges[spos].begin(), new_edges[spos].end(),
                                       std::back_inserter(possibleEdges[eids[spos]]));
                    }
                }
            }

            std::cout << "Capacity of current edges is " << current_edges[0].capacity() << " and " << current_edges[1].capacity() << std::endl;
        }

        _query_sorter.sort();
    }

    void loadEdgeExistenceInformation() {
        _edge_existence_pq.clear();
        _edge_existence_successors.clear();

        typename edge_vector::bufreader_type edgeReader(_edges);
        while (!_query_sorter.empty()) {
            if (edgeReader.empty() || *edgeReader >= _query_sorter->e) { // found edge or went past it (does not exist)
                // first request for the edge - give existence info if edge exists
                auto lastQuery = *_query_sorter;
                bool edgeExists = (!edgeReader.empty() && *edgeReader == _query_sorter->e);

                // found requested edge - advance reader
                if (!edgeReader.empty() && *edgeReader == _query_sorter->e) {
                    ++edgeReader;
                }

                // iterate over all queries with the same edge sorted by sid in decreasing order

                bool foundTargetEdge = false; // if we already found a swap where the edge is a target
                while (!_query_sorter.empty() && _query_sorter->e == lastQuery.e) {
                    // skip duplicates and first result
                    if (_query_sorter->sid != lastQuery.sid && foundTargetEdge) {
                        // We only need existence information for targets but when it is a source edge it might be deleted,
                        // therefore store successor information whenever an edge occurs as target after the current swap
                        _edge_existence_successors.push_back(edge_existence_successor_t {_query_sorter->sid, lastQuery.e, lastQuery.sid});
                    }

                    lastQuery = *_query_sorter;
                    foundTargetEdge = (foundTargetEdge || ! _query_sorter->forward_only);
                    ++_query_sorter;
                }

                // If the edge is target edge for any swap, we need to store its current status for the first swap the edge is part of
                if (foundTargetEdge) {
                    #ifndef NDEBUG
                    _edge_existence_pq.push_back(edge_existence_answer_t {lastQuery.sid, lastQuery.e, edgeExists});
                    #else
                    if (edgeExists) {
                        _edge_existence_pq.push_back(edge_existence_answer_t {lastQuery.sid, lastQuery.e});
                    }
                    #endif
                }
            } else { // query edge might be after the current edge, advance edge reader to check
                ++edgeReader;
            }
        }

        _query_sorter.clear();
        std::sort(_edge_existence_successors.begin(), _edge_existence_successors.end());
        std::make_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
    }

    void performSwaps(typename debug_vector::bufwriter_type &debug_vector_writer) {
        auto edge_existence_succ_it = _edge_existence_successors.begin();
        std::vector<edge_t> current_existence;
#ifndef NDEBUG
        std::vector<edge_t> current_missing;
#endif
        for (auto s_it = _current_swaps.begin(); s_it != _current_swaps.end(); ++s_it) {
            const auto& eids = s_it->edges();
            const auto sid = (s_it - _current_swaps.begin());
            edge_t s_edges[2] = {_edges_in_current_swaps[eids[0]], _edges_in_current_swaps[eids[1]]};
            edge_t t_edges[2];
            current_existence.clear();
#ifndef NDEBUG
            current_missing.clear();
#endif

            SwapResult result;

            //std::cout << "Testing swap of " << e0.first << ", " << e0.second << " and " << e1.first << ", " << e1.second << std::endl;

            std::tie(t_edges[0], t_edges[1]) = _swap_edges(s_edges[0], s_edges[1], s_it->direction());

            assert(_edge_existence_pq.empty() || _edge_existence_pq.front().sid >= sid);

            while (!_edge_existence_pq.empty() && _edge_existence_pq.front().sid == sid) {
#ifdef NDEBUG
                current_existence.push_back(_edge_existence_pq.front().e);
#else
                if (_edge_existence_pq.front().exists) {
                    current_existence.push_back(_edge_existence_pq.front().e);
                } else {
                    current_missing.push_back(_edge_existence_pq.front().e);
                }
#endif
                std::pop_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
                _edge_existence_pq.pop_back();
            }

            //std::cout << "Target edges " << t0.first << ", " << t0.second << " and " << t1.first << ", " << t1.second << std::endl;
            {
                // compute whether swap can be performed and write debug info out
                result.edges[0] = t_edges[0];
                result.edges[1] = t_edges[1];
                result.loop = (t_edges[0].first == t_edges[0].second || t_edges[1].first == t_edges[1].second);

                result.conflictDetected[0] = std::binary_search(current_existence.begin(), current_existence.end(), t_edges[0]);
                result.conflictDetected[1] = std::binary_search(current_existence.begin(), current_existence.end(), t_edges[1]);
#ifndef NDEBUG
                if (!result.loop) {
                    if (!result.conflictDetected[0]) {
                        assert(std::binary_search(current_missing.begin(), current_missing.end(), t_edges[0]));
                    }

                    if (!result.conflictDetected[1]) {
                        assert(std::binary_search(current_missing.begin(), current_missing.end(), t_edges[1]));
                    }
                }
#endif

                result.performed = !result.loop && !(result.conflictDetected[0] || result.conflictDetected[1]);
                result.normalize();

                debug_vector_writer << result;
            }

            //std::cout << result << std::endl;

            //assert(result.loop || ((existsEdge.find(t_edges[0]) != existsEdge.end() && existsEdge.find(t_edges[1]) != existsEdge.end())));

            if (!result.performed) {
                t_edges[0] = s_edges[0];
                t_edges[1] = s_edges[1];
            } else {
                _edges_in_current_swaps[eids[0]] = t_edges[0];
                _edges_in_current_swaps[eids[1]] = t_edges[1];
            }

            while (edge_existence_succ_it != _edge_existence_successors.end() && edge_existence_succ_it->from_sid == sid) {
                if (edge_existence_succ_it->e == t_edges[0] || edge_existence_succ_it->e == t_edges[1]) {
                    // target edges always exist (might be source if no swap has been performed)
#ifdef NDEBUG
                    _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e});
#else
                    _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e, true});
#endif
                    std::push_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
                } else if (edge_existence_succ_it->e == s_edges[0] || edge_existence_succ_it->e == s_edges[1]) {
                    // source edges never exist (if no swap has been performed or source = target, this has been handled above)
#ifndef NDEBUG
                    _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e, false});
                    std::push_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
#endif
                } else {
#ifdef NDEBUG
                    if (std::binary_search(current_existence.begin(), current_existence.end(), edge_existence_succ_it->e)) {
                        _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e});
                        std::push_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
                    }
#else
                    if (std::binary_search(current_existence.begin(), current_existence.end(), edge_existence_succ_it->e)) {
                        _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e, true});
                    } else {
                        assert(std::binary_search(current_missing.begin(), current_missing.end(), edge_existence_succ_it->e));
                        _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e, false});
                    }

                    std::push_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
#endif
                }

                ++edge_existence_succ_it;
            }
        }
    };


    void updateEdgesAndLoadSwapsWithEdgesAndSuccessors(typename swap_vector::bufreader_type &swapReader) {
        // stores load requests with information who requested the edge
        struct edge_swap_t {
            edgeid_t eid;
            int_t sid;
            unsigned char spos;

            DECL_LEX_COMPARE(edge_swap_t, eid, sid, spos);
        };

        // load edge endpoints for edges in the swap set
        std::vector<edge_swap_t> edgeLoadRequests;
        edgeLoadRequests.reserve(_num_swaps_per_iteration * 2);

        // copy old edge ids for writing back
        std::vector<edgeid_t> old_edge_ids;
        old_edge_ids.swap(_edge_ids_in_current_swaps);
        _edge_ids_in_current_swaps.reserve(_num_swaps_per_iteration * 2);

        // copy updated edges for writing back
        std::vector<edge_t> updated_edges;
        updated_edges.swap(_edges_in_current_swaps);
        std::sort(updated_edges.begin(), updated_edges.end());
        _edges_in_current_swaps.reserve(_num_swaps_per_iteration * 2);

        _current_swaps.clear();
        _current_swaps.reserve(_num_swaps_per_iteration);

        _swap_has_successor[0].clear();
        _swap_has_successor[0].resize(_num_swaps_per_iteration);
        _swap_has_successor[1].clear();
        _swap_has_successor[1].resize(_num_swaps_per_iteration);

        for (int_t i = 0; i < _num_swaps_per_iteration && !swapReader.empty(); ++i, ++swapReader) {
            _current_swaps.emplace_back(*swapReader);
            edgeLoadRequests.push_back(edge_swap_t {swapReader->edges()[0], i, 0});
            edgeLoadRequests.push_back(edge_swap_t {swapReader->edges()[1], i, 1});
        }


        std::cout << "Requesting " << edgeLoadRequests.size() << " non-unique edges for internal swaps" << std::endl;
        std::sort(edgeLoadRequests.begin(), edgeLoadRequests.end());


        { // load edges from EM. Generates successor information and swap_edges information (for the first edge in the chain).
            int_t int_eid = 0;
            edge_vector output_vector;
            output_vector.reserve(_edges.size());

            typename edge_vector::bufreader_type edge_reader(_edges);
            typename edge_vector::bufwriter_type writer(output_vector);
            auto request_it = edgeLoadRequests.begin();

            auto old_e = old_edge_ids.begin();
            auto new_e = updated_edges.begin();

            int_t read_id = 0;
            edge_t cur_e;

            for (int_t id = 0; !edge_reader.empty() || new_e != updated_edges.end(); ++id) {
                // Skip old edges
                while (old_e != old_edge_ids.end() && *old_e == read_id) {
                    ++edge_reader;
                    ++read_id;
                    ++old_e;
                }

                // merge update edges and read edges
                if (new_e != updated_edges.end() && (edge_reader.empty() || *new_e < *edge_reader)) {
                    cur_e = *new_e;
                    writer << cur_e;
                    ++new_e;
                }  else {
                    if (edge_reader.empty()) { // due to the previous while loop both could be empty now
                        break; // abort the loop as we do not have any edges to process anymore.
                    }

                    cur_e = *edge_reader;
                    writer << cur_e;
                    ++read_id;
                    ++edge_reader;
                }

                if (request_it != edgeLoadRequests.end() && request_it->eid == id) {
                    _edge_ids_in_current_swaps.push_back(request_it->eid);
                    _edges_in_current_swaps.push_back(cur_e);
                    assert(int_eid == _edges_in_current_swaps.size() - 1);

                    // set edge id to internal edge id
                    _current_swaps[request_it->sid].edges()[request_it->spos] = int_eid;

                    auto lastSwap = *request_it;
                    ++request_it;

                    // further requests for the same swap - store successor information
                    while (request_it != edgeLoadRequests.end() &&  request_it->eid == id) {
                        // set edge id to internal edge id
                        _current_swaps[request_it->sid].edges()[request_it->spos] = int_eid;
                        _swap_has_successor[lastSwap.spos][lastSwap.sid] = true;
                        lastSwap = *request_it;
                        ++request_it;
                    }

                    ++int_eid;
                }
            }

            writer.finish();
            _edges.swap(output_vector);
        }
    };

public:
    EdgeSwapInternalSwaps() = delete;
    EdgeSwapInternalSwaps(const EdgeSwapInternalSwaps &) = delete;

    //! Swaps are performed during constructor.
    //! @param edges  Edge vector changed in-place
    //! @param swaps  Read-only swap vector
    EdgeSwapInternalSwaps(edge_vector & edges, swap_vector & swaps, int_t num_swaps_per_iteration = 1000000) :
        EdgeSwapBase(),
        _edges(edges),
        _swaps(swaps),
        _num_swaps_per_iteration(num_swaps_per_iteration),
        _query_sorter(typename GenericComparatorStruct<edge_existence_request_t>::Ascending(), SORTER_MEM)
    {}

    void run() {
        typename swap_vector::bufreader_type reader(_swaps);
        typename debug_vector::bufwriter_type debug_vector_writer(_results);

        bool show_stats = true;

        _start_stats(show_stats);

        updateEdgesAndLoadSwapsWithEdgesAndSuccessors(reader);

        _report_stats("load swaps", show_stats);

        while (!_current_swaps.empty()) {
            // std::cout << "Identified " << _swap_successors.size() << " duplications of edge ids which need to be handled later." << std::endl;

            simulateSwapsAndGenerateEdgeExistenceQuery();

            _report_stats("swap simulation", show_stats);

            std::cout << "Requesting " << _query_sorter.size() << " (possibly non-unique) possible conflict edges" << std::endl;

            loadEdgeExistenceInformation();

            _report_stats("load existence information", show_stats);

            std::cout << "Loaded " << _edge_existence_pq.size() << " existence values" << std::endl;
            std::cout << "Values might be forwarded " << _edge_existence_successors.size() << " times" << std::endl;

            std::cout << "Doing swaps" << std::endl;

            // do swaps
            performSwaps(debug_vector_writer);

            _report_stats("perform swaps", show_stats);

            std::cout << "Capacity of internal edge existence PQ: " << _edge_existence_pq.capacity() << std::endl;

            // update edge vector
            updateEdgesAndLoadSwapsWithEdgesAndSuccessors(reader);

            _report_stats("write back and load next swaps", show_stats);
            std::cout << "Finished swap phase, writing back and loading edges" << std::endl;
        }


        debug_vector_writer.finish();
   }

   //! The i-th entry of this vector corresponds to the i-th
   //! swap provided to the constructor
   debug_vector & debugVector() {
      return _results;
   }
};

// prevent implicit instantiation for default case
extern template class EdgeSwapInternalSwaps<stxxl::vector<edge_t>, stxxl::vector<SwapDescriptor>>;
