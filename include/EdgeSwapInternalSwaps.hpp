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
        _num_swaps_per_iteration(num_swaps_per_iteration)
    {}

    void run() {
        typename swap_vector::bufreader_type reader(_swaps);

        std::vector<swap_descriptor> currentSwaps;
        currentSwaps.reserve(_num_swaps_per_iteration);

        typename debug_vector::bufwriter_type debug_vector_writer(_results);

        while (!reader.empty()) {
            currentSwaps.clear();

            // stores load requests with information who requested the edge
            struct edge_swap_t {
                edgeid_t eid;
                int_t sid;
                unsigned char spos;

                DECL_LEX_COMPARE(edge_swap_t, eid, sid, spos);
            };

            // load edge endpoints for edges in the swap set
            // TODO: this should already be done in the writeback phase
            stxxl::sorter<edge_swap_t, typename GenericComparatorStruct<edge_swap_t>::Ascending> swapSorter(typename GenericComparatorStruct<edge_swap_t>::Ascending(), SORTER_MEM);
            for (int_t i = 0; i < _num_swaps_per_iteration && !reader.empty(); ++i) {
                currentSwaps.emplace_back(*reader);
                swapSorter.push(edge_swap_t {reader->edges()[0], i, 0});
                swapSorter.push(edge_swap_t {reader->edges()[1], i, 1});
                ++reader;
            }

            std::cout << "Requesting " << swapSorter.size() << " non-unique edges for internal swaps" << std::endl;
            swapSorter.sort();

            // stores for a swap and the position in the swap (0,1) the edge
            struct swap_edge_t {
                int_t sid;
                unsigned char spos;
                edge_t e;

                DECL_LEX_COMPARE(swap_edge_t, sid, spos, e);
            };

            // stores successors of swaps in terms of edge dependencies
            struct swap_successor_t {
                int_t from_sid;
                unsigned char from_spos;
                int_t to_sid;
                unsigned char to_spos;

                DECL_LEX_COMPARE(swap_successor_t, from_sid, from_spos);
            };

            std::vector<swap_edge_t> swap_edges;
            swap_edges.reserve(currentSwaps.size() * 2); // maximum size, is actually smaller (a bit) because for duplicate edge ids only the first swap gets the information.
            std::vector<swap_successor_t> swap_successors;


            { // load edges from EM. Generates successor information and swap_edges information (for the first edge in the chain).
                int_t id = 0;

                typename edge_vector::bufreader_type edge_reader(_edges);
                while (!edge_reader.empty()) {
                    if (swapSorter.empty()) break;

                    if (swapSorter->eid == id) {
                        swap_edges.push_back(swap_edge_t {swapSorter->sid, swapSorter->spos, *edge_reader});
                        auto lastSwap = *swapSorter;
                        ++swapSorter;

                        // further requests for the same swap - store successor information
                        while (!swapSorter.empty() && swapSorter->eid == id) {
                            swap_successors.push_back(swap_successor_t {lastSwap.sid, lastSwap.spos, swapSorter->sid, swapSorter->spos});
                            lastSwap = *swapSorter;
                            ++swapSorter;
                        }
                    }

                    ++edge_reader;
                    ++id;
                }
            }

            std::sort(swap_successors.begin(), swap_successors.end()); // sort successor information - we only need this read-only from now on
            std::make_heap(swap_edges.begin(), swap_edges.end(), std::greater<swap_edge_t>()); // make heap from swap_edges, we need to use this for sending edges to other swaps

            auto try_swap_edges = swap_edges; // copy heap, this is for finding out all possible conflict edges


            std::cout << "Identified " << swap_successors.size() << " duplications of edge ids which need to be handled later." << std::endl;

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

            stxxl::sorter<edge_existence_request_t, typename GenericComparatorStruct<edge_existence_request_t>::Ascending> querySorter(typename GenericComparatorStruct<edge_existence_request_t>::Ascending(), SORTER_MEM); // Query of possible conflict edges. This may be large (too large...)

            { // find possible conflicts
                auto succ_it = swap_successors.begin();

                std::vector<edge_t> current_edges[2]; // edges for the current swap (position 0 and 1)

                // construct possible conflict pairs
                for (auto s_it = currentSwaps.begin(); s_it != currentSwaps.end(); ++s_it) {
                    const auto sid = (s_it - currentSwaps.begin());
                    current_edges[0].clear();
                    current_edges[1].clear();

                    // there can be only two succesors - one for each position
                    std::tuple<int_t, unsigned char> current_successors[2] = {std::make_tuple(0, 0), std::make_tuple(0, 0)};

                    // load these two successors (if any)
                    while (succ_it != swap_successors.end() && succ_it->from_sid == sid) {
                        current_successors[succ_it->from_spos] = std::make_tuple(succ_it->to_sid, succ_it->to_spos);
                        ++succ_it;
                    }

                    assert(!try_swap_edges.empty() && try_swap_edges.front().sid == sid);

                    // load all edges for the current swap from the PQ
                    while (!try_swap_edges.empty() && try_swap_edges.front().sid == sid) {
                        auto spos = try_swap_edges.front().spos;
                        auto e = try_swap_edges.front().e;
                        std::pop_heap(try_swap_edges.begin(), try_swap_edges.end(), std::greater<swap_edge_t>());

                        if (!current_edges[spos].empty() && e == current_edges[spos].back()) { // duplicate edge! FIXME why are duplicates created? Find out and prevent them maybe?
                            try_swap_edges.pop_back();
                        } else {
                            current_edges[spos].push_back(e);
                            /* request all edges that might be deleted here as we need to correctly forward this information to other swaps that need the information
                             * Note that either we know that the edge exists as it is the source of the swap or
                             * the same edge has already been requested by a previous swap and the first one of this chain requests the actual information.
                             */
                            querySorter.push(edge_existence_request_t {e, sid, true});

                            // if this is no duplicate and we have a successor, send all edges also to the successor
                            if (std::get<0>(current_successors[spos])) {
                                try_swap_edges.back().sid = std::get<0>(current_successors[spos]);
                                try_swap_edges.back().spos = std::get<1>(current_successors[spos]);
                                std::push_heap(try_swap_edges.begin(), try_swap_edges.end(), std::greater<swap_edge_t>());
                            } else {
                                try_swap_edges.pop_back();
                            }
                        }

                    }

                    assert(try_swap_edges.empty() || try_swap_edges.front().sid >= sid);

                    assert(!current_edges[0].empty());
                    assert(!current_edges[1].empty());

                    // Iterate over all pairs of edges and try the swap
                    for (const auto & e0 : current_edges[0]) {
                        for (const auto &e1 : current_edges[1]) {
                            edge_t t[2];
                            std::tie(t[0], t[1]) = _swap_edges(e0, e1, s_it->direction());

                            // record the two conflict edges unless the conflict is trivial
                            if (t[0].first != t[0].second && t[1].first != t[1].second) {
                                for (unsigned char spos = 0; spos < 2; ++spos) {
                                    if (std::get<0>(current_successors[spos])) { // forward the new candidates for our two edge ids to possible successors
                                        try_swap_edges.push_back(swap_edge_t {std::get<0>(current_successors[spos]), std::get<1>(current_successors[spos]), t[spos]});
                                        std::push_heap(try_swap_edges.begin(), try_swap_edges.end(), std::greater<swap_edge_t>());
                                    }

                                    // record the query
                                    querySorter.push(edge_existence_request_t {t[spos], sid, false});
                                }
                            }
                        }
                    }

                    assert(try_swap_edges.empty() || try_swap_edges.front().sid >= sid);
                }
            }

            assert(try_swap_edges.empty());
            std::cout << "Capacity of internal PQ: " << try_swap_edges.capacity() << std::endl;

            querySorter.sort();

            std::cout << "Requesting " << querySorter.size() << " (possibly non-unique) possible conflict edges" << std::endl;

            struct edge_existence_answer_t {
                int_t sid;
                edge_t e;
#ifndef NDEBUG
                bool exists;
#endif
                DECL_LEX_COMPARE(edge_existence_answer_t, sid, e);
            };

            struct edge_existence_successor_t {
                int_t from_sid;
                edge_t e;
                int_t to_sid;
                DECL_LEX_COMPARE(edge_existence_successor_t, from_sid, e);
            };

            stxxl::sorter<edge_existence_successor_t, typename GenericComparatorStruct<edge_existence_successor_t>::Ascending> edge_existence_successors(typename GenericComparatorStruct<edge_existence_successor_t>::Ascending(), SORTER_MEM);

            std::vector<edge_existence_answer_t> edge_existence_pq;

            typename edge_vector::bufreader_type edgeReader(_edges);
            while (!querySorter.empty()) {
                if (edgeReader.empty() || *edgeReader >= querySorter->e) { // found edge or went past it (does not exist)
                    // first request for the edge - give existence info if edge exists
                    auto lastQuery = *querySorter;
                    bool edgeExists = (!edgeReader.empty() && *edgeReader == querySorter->e);

                    // found requested edge - advance reader
                    if (!edgeReader.empty() && *edgeReader == querySorter->e) {
                        ++edgeReader;
                    }

                    // iterate over all queries with the same edge sorted by sid in decreasing order

                    bool foundTargetEdge = false; // if we already found a swap where the edge is a target
                    while (!querySorter.empty() && querySorter->e == lastQuery.e) {
                        // skip duplicates and first result
                        if (querySorter->sid != lastQuery.sid && foundTargetEdge) {
                            // We only need existence information for targets but when it is a source edge it might be deleted,
                            // therefore store successor information whenever an edge occurs as target after the current swap
                            edge_existence_successors.push(edge_existence_successor_t {querySorter->sid, lastQuery.e, lastQuery.sid});
                        }

                        lastQuery = *querySorter;
                        foundTargetEdge = (foundTargetEdge || ! querySorter->forward_only);
                        ++querySorter;
                    }

                    // If the edge is target edge for any swap, we need to store its current status for the first swap the edge is part of
                    if (foundTargetEdge) {
                        #ifndef NDEBUG
                        edge_existence_pq.push_back(edge_existence_answer_t {lastQuery.sid, lastQuery.e, edgeExists});
                        #else
                        if (edgeExists) {
                            edge_existence_pq.push_back(edge_existence_answer_t {lastQuery.sid, lastQuery.e});
                        }
                        #endif
                    }
                } else { // query edge might be after the current edge, advance edge reader to check
                    ++edgeReader;
                }
            }

            querySorter.finish();
            edge_existence_successors.sort();
            std::make_heap(edge_existence_pq.begin(), edge_existence_pq.end(), std::greater<edge_existence_answer_t>());

            std::cout << "Loaded " << edge_existence_pq.size() << " existence values" << std::endl;
            std::cout << "Values might be forwarded " << edge_existence_successors.size() << " times" << std::endl;

            std::cout << "Doing swaps" << std::endl;

            std::vector<edgeid_t> deletions; // store old, deleted edge ids
            deletions.reserve(swap_edges.size());
            std::vector<edge_t> insertions; // store new edges (without ids)
            deletions.reserve(swap_edges.size());

            // do swaps
            {
                // uses swap_edges PQ for list of edges of each swap (already create above)
                // each swap sends edges to its successor(s) in swap_successors
                // the last one in the chain then records the final result
                auto succ_it = swap_successors.begin();
                std::vector<edge_t> current_existence;
#ifndef NDEBUG
                std::vector<edge_t> current_missing;
#endif
                for (auto s_it = currentSwaps.begin(); s_it != currentSwaps.end(); ++s_it) {
                    const auto& eids = s_it->edges();
                    const auto sid = (s_it - currentSwaps.begin());
                    edge_t s_edges[2];
                    edge_t t_edges[2];
                    current_existence.clear();
#ifndef NDEBUG
                    current_missing.clear();
#endif

                    for (unsigned char spos = 0; spos < 2; ++spos) {
                        assert(swap_edges.front().sid == sid);
                        assert(swap_edges.front().spos == spos);
                        s_edges[spos] = swap_edges.front().e;

                        std::pop_heap(swap_edges.begin(), swap_edges.end(), std::greater<swap_edge_t>());
                        swap_edges.pop_back();
                    }


                    SwapResult result;

                    //std::cout << "Testing swap of " << e0.first << ", " << e0.second << " and " << e1.first << ", " << e1.second << std::endl;

                    std::tie(t_edges[0], t_edges[1]) = _swap_edges(s_edges[0], s_edges[1], s_it->direction());

                    assert(edge_existence_pq.empty() || edge_existence_pq.front().sid >= sid);

                    while (!edge_existence_pq.empty() && edge_existence_pq.front().sid == sid) {
#ifdef NDEBUG
                        current_existence.push_back(edge_existence_pq.front().e);
#else
                        if (edge_existence_pq.front().exists) {
                            current_existence.push_back(edge_existence_pq.front().e);
                        } else {
                            current_missing.push_back(edge_existence_pq.front().e);
                        }
#endif
                        std::pop_heap(edge_existence_pq.begin(), edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
                        edge_existence_pq.pop_back();
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
                    }

                    while (!edge_existence_successors.empty() && edge_existence_successors->from_sid == sid) {
                        const auto &succ = *edge_existence_successors;
                        if (succ.e == t_edges[0] || succ.e == t_edges[1]) {
                            // target edges always exist (might be source if no swap has been performed)
#ifdef NDEBUG
                            edge_existence_pq.push_back(edge_existence_answer_t {succ.to_sid, succ.e});
#else
                            edge_existence_pq.push_back(edge_existence_answer_t {succ.to_sid, succ.e, true});
#endif
                            std::push_heap(edge_existence_pq.begin(), edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
                        } else if (succ.e == s_edges[0] || succ.e == s_edges[1]) {
                            // source edges never exist (if no swap has been performed or source = target, this has been handled above)
#ifndef NDEBUG
                            edge_existence_pq.push_back(edge_existence_answer_t {succ.to_sid, succ.e, false});
                            std::push_heap(edge_existence_pq.begin(), edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
#endif
                        } else {
#ifdef NDEBUG
                        if (std::binary_search(current_existence.begin(), current_existence.end(), succ.e)) {
                            edge_existence_pq.push_back(edge_existence_answer_t {succ.to_sid, succ.e});
                            std::push_heap(edge_existence_pq.begin(), edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
                        }
#else
                        if (std::binary_search(current_existence.begin(), current_existence.end(), succ.e)) {
                            edge_existence_pq.push_back(edge_existence_answer_t {succ.to_sid, succ.e, true});
                        } else {
                            assert(std::binary_search(current_missing.begin(), current_missing.end(), succ.e));
                            edge_existence_pq.push_back(edge_existence_answer_t {succ.to_sid, succ.e, false});
                        }

                        std::push_heap(edge_existence_pq.begin(), edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
#endif
                        }

                        ++edge_existence_successors;
                    }

                    for (unsigned char spos = 0; spos < 2; ++spos) {
                        if (succ_it != swap_successors.end() && succ_it->from_sid == sid && succ_it->from_spos == spos) {
                            swap_edges.push_back(swap_edge_t {succ_it->to_sid, succ_it->to_spos, t_edges[spos]});
                            std::push_heap(swap_edges.begin(), swap_edges.end(), std::greater<swap_edge_t>());
                            ++succ_it;
                        } else { // if (result.performed) { // if we knew we are not a successor, we could skip this here!
                            // no successor - final edge for eid!
                            deletions.push_back(eids[spos]);
                            insertions.push_back(t_edges[spos]);
                        }
                    }
                }
            }

            // write result back into edge vector. Inserts all edges in insertions, deletes all edge ids from deletions.
            {
                edge_vector output_vector;
                output_vector.reserve(_edges.size());
                typename edge_vector::bufwriter_type writer(output_vector);
                typename edge_vector::bufreader_type edge_reader(_edges);

                std::sort(insertions.begin(), insertions.end());
                std::sort(deletions.begin(), deletions.end());

                auto old_e = deletions.begin();
                auto new_e = insertions.begin();

                int_t read_id = 0;

                while (!edge_reader.empty() || new_e != insertions.end()) {
                    // Skip elements that were already read
                    while (old_e != deletions.end() && *old_e == read_id) {
                        ++edge_reader;
                        ++read_id;
                        ++old_e;
                    }

                    if (new_e != insertions.end() && (edge_reader.empty() || *new_e < *edge_reader)) {
                        writer << *new_e;
                        ++new_e;
                    }  else if (!edge_reader.empty()) { // due to the previous while loop both could be empty now
                        writer << *edge_reader;
                        ++read_id;
                        ++edge_reader;
                    }
                }

                writer.finish();
                _edges.swap(output_vector);
            }

            std::cout << "Finished swap phase and writing back" << std::endl;
            std::cout << "Capacity of internal edge existence PQ: " << edge_existence_pq.capacity() << std::endl;
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
