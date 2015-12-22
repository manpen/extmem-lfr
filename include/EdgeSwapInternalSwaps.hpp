#pragma once

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stx/btree_set>
#include <stx/btree_map>
#include <tuple>
#include <set>
#include <vector>
#include <stack>
#include <functional>
#include "EdgeSwapBase.h"
#include "GenericComparator.h"
#include <TupleHelper.h>
#include <algorithm>

template <class EdgeVector = stxxl::vector<node_t>, class SwapVector = stxxl::vector<SwapDescriptor>>
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

            struct edge_swap_t {
                edgeid_t eid;
                int_t sid;
                unsigned char spos;

                DECL_LEX_COMPARE(edge_swap_t, eid, sid, spos);
            };

            // load edge endpoints for edges in the swap set
            stxxl::sorter<edge_swap_t, typename GenericComparatorStruct<edge_swap_t>::Ascending> swapSorter(typename GenericComparatorStruct<edge_swap_t>::Ascending(), 128*IntScale::Mi);
            for (int_t i = 0; i < _num_swaps_per_iteration && !reader.empty(); ++i) {
                currentSwaps.emplace_back(*reader);
                swapSorter.push(edge_swap_t {reader->edges()[0], i, 0});
                swapSorter.push(edge_swap_t {reader->edges()[1], i, 1});
                ++reader;
            }

            /*
            Ideas:

            * Sort in internal memory!!!
            * Compute dependency chain of edge ids
            * Only store those in btree/PQ that have a successor!
            * Instead of a Btree use a PQ, we know the successor i.e. the next node that will access the edge information and can index accordingly.
            * Replace EdgeVectorCache by PQ (initially identical to previous PQ)

            * Also compute edge dependecy PQ
            */

            std::cout << "Requesting " << swapSorter.size() << " non-unique edges for internal swaps" << std::endl;
            swapSorter.sort();

            struct swap_edge_t {
                int_t sid;
                unsigned char spos;
                edge_t e;

                DECL_LEX_COMPARE(swap_edge_t, sid, spos, e);
            };

            struct swap_successor_t {
                int_t from_sid;
                unsigned char from_spos;
                int_t to_sid;
                unsigned char to_spos;

                DECL_LEX_COMPARE(swap_successor_t, from_sid, from_spos);
            };

            std::vector<swap_edge_t> swap_edges;
            swap_edges.reserve(currentSwaps.size() * 2);
            std::vector<swap_successor_t> swap_successors;


            {
                int_t id = 0;

                typename edge_vector::bufreader_type edge_reader(_edges);
                while (!edge_reader.empty()) {
                    if (swapSorter.empty()) break;

                    if (swapSorter->eid == id) {
                        swap_edges.push_back(swap_edge_t {swapSorter->sid, swapSorter->spos, *edge_reader});
                        auto lastSwap = *swapSorter;
                        ++swapSorter;

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

            std::sort(swap_successors.begin(), swap_successors.end());
            std::make_heap(swap_edges.begin(), swap_edges.end(), std::greater<swap_edge_t>());

            auto try_swap_edges = swap_edges;


            std::cout << "Identified " << swap_successors.size() << " duplications of edge ids which need to be handled later." << std::endl;


            stxxl::sorter<edge_t, EdgeComparator> querySorter(EdgeComparator(), 128*IntScale::Mi);

            {
                auto succ_it = swap_successors.begin();

                std::vector<edge_t> current_edges[2];

                // construct possible conflict pairs
                for (auto s_it = currentSwaps.begin(); s_it != currentSwaps.end(); ++s_it) {
                    const auto sid = (s_it - currentSwaps.begin());
                    current_edges[0].clear();
                    current_edges[1].clear();

                    std::tuple<int_t, unsigned char> current_successors[2] = {std::make_tuple(0, 0), std::make_tuple(0, 0)};

                    while (succ_it != swap_successors.end() && succ_it->from_sid == sid) {
                        current_successors[succ_it->from_spos] = std::make_tuple(succ_it->to_sid, succ_it->to_spos);
                        ++succ_it;
                    }

                    assert(!try_swap_edges.empty() && try_swap_edges.front().sid == sid);

                    while (!try_swap_edges.empty() && try_swap_edges.front().sid == sid) {
                        auto spos = try_swap_edges.front().spos;
                        auto e = try_swap_edges.front().e;
                        std::pop_heap(try_swap_edges.begin(), try_swap_edges.end(), std::greater<swap_edge_t>());

                        if (!current_edges[spos].empty() && e == current_edges[spos].back()) { // duplicate edge! FIXME why are duplicates created? Find out and prevent them maybe?
                            try_swap_edges.pop_back();
                        } else {
                            current_edges[spos].push_back(e);

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

                    for (const auto & e0 : current_edges[0]) {
                        for (const auto &e1 : current_edges[1]) {
                            edge_t t[2];
                            std::tie(t[0], t[1]) = _swap_edges(e0, e1, s_it->direction());

                            for (unsigned char spos = 0; spos < 2; ++spos) {
                                if (t[spos].first != t[spos].second) { // no loop
                                    if (std::get<0>(current_successors[spos])) {
                                        try_swap_edges.push_back(swap_edge_t {std::get<0>(current_successors[spos]), std::get<1>(current_successors[spos]), t[spos]});
                                        std::push_heap(try_swap_edges.begin(), try_swap_edges.end(), std::greater<swap_edge_t>());
                                    }

                                    querySorter.push(t[spos]);
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

            stxxl::stream::unique<decltype(querySorter)> uniqueQuery(querySorter);

            // check if any of the requested edges exists, store booleans
            #ifdef NDEBUG
            stx::btree_set<edge_t> existsEdge;
            #else
            stx::btree_map<edge_t, bool> existsEdge;
            #endif

            typename edge_vector::bufreader_type edgeReader(_edges);
            while (!uniqueQuery.empty()) {
                if (edgeReader.empty() || *edgeReader > *uniqueQuery) { // edge reader went past the query - edge does not exist!
                    #ifndef NDEBUG
                        existsEdge.insert(existsEdge.end(), std::make_pair(*uniqueQuery, false));
                    #endif
                    //std::cout << uniqueQuery->first << ", " << uniqueQuery->second << " does not exist" << std::endl;
                    ++uniqueQuery;
                } else if (*edgeReader == *uniqueQuery) { // found requested edge - advance both
                    #ifdef NDEBUG
                        existsEdge.insert(existsEdge.end(), *edgeReader);
                    #else
                        existsEdge.insert(existsEdge.end(), std::make_pair(*edgeReader, true));
                    #endif
                    //std::cout << edgeReader->first << ", " << edgeReader->second << " exists" << std::endl;
                    ++edgeReader;
                    ++uniqueQuery;
                } else { // query edge might be after the current edge, advance edge reader to check
                    ++edgeReader;
                }
            }

            std::cout << "Loaded " << existsEdge.size() << " existence values" << std::endl;

            std::cout << "Doing swaps" << std::endl;

            std::vector<edgeid_t> deletions;
            deletions.reserve(swap_edges.size());
            std::vector<edge_t> insertions;
            deletions.reserve(swap_edges.size());

            // do swaps
            {
                auto succ_it = swap_successors.begin();
                for (auto s_it = currentSwaps.begin(); s_it != currentSwaps.end(); ++s_it) {
                    const auto& eids = s_it->edges();
                    const auto sid = (s_it - currentSwaps.begin());
                    edge_t s_edges[2];
                    edge_t t_edges[2];

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

                    //std::cout << "Target edges " << t0.first << ", " << t0.second << " and " << t1.first << ", " << t1.second << std::endl;
                    {
                        // compute whether swap can be performed and write debug info out
                        result.edges[0] = t_edges[0];
                        result.edges[1] = t_edges[1];
                        result.loop = (t_edges[0].first == t_edges[0].second || t_edges[1].first == t_edges[1].second);

                        #ifdef NDEBUG
                        result.conflictDetected[0] = existsEdge.exists(t_edges[0]);
                        result.conflictDetected[1] = existsEdge.exists(t_edges[1]);
                        #else
                        result.conflictDetected[0] = existsEdge[t_edges[0]];
                        result.conflictDetected[1] = existsEdge[t_edges[1]];
                        #endif

                        result.performed = !result.loop && !(result.conflictDetected[0] || result.conflictDetected[1]);
                        result.normalize();

                        debug_vector_writer << result;
                    }

                    //std::cout << result << std::endl;

                    assert(result.loop || ((existsEdge.find(t_edges[0]) != existsEdge.end() && existsEdge.find(t_edges[1]) != existsEdge.end())));

                    if (result.performed) {
                        #ifdef NDEBUG
                        existsEdge.erase(s_edges[0]);
                        existsEdge.erase(s_edges[1]);
                        existsEdge.insert(t_edges[0]);
                        existsEdge.insert(t_edges[1]);
                        #else
                        existsEdge[s_edges[0]] = false;
                        existsEdge[s_edges[1]] = false;
                        existsEdge[t_edges[0]] = true;
                        existsEdge[t_edges[1]] = true;
                        #endif
                    } else {
                        t_edges[0] = s_edges[0];
                        t_edges[1] = s_edges[1];
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
        }

        debug_vector_writer.finish();
   }

   //! The i-th entry of this vector corresponds to the i-th
   //! swap provided to the constructor
   debug_vector & debugVector() {
      return _results;
   }
};
