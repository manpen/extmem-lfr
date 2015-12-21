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
#include "EdgeVectorCache.h"
#include "GenericComparator.h"

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

    struct PossibleEdge {
        edgeid_t eid;
        edge_t e;

        bool operator<(const PossibleEdge &o) const {
            return std::tie(eid, e) < std::tie(o.eid, o.e);
        };

        operator edge_t() const {
            return e;
        };
    };

    template <typename InputStream>
    struct RememberingEdgeIdUnique {
        using value_type = edgeid_t;

        stx::btree_set<value_type> duplicates;
        InputStream &input;

        RememberingEdgeIdUnique(InputStream &input) : input(input) {};
        const value_type & operator * () const { return *input; };
        const value_type * operator -> () const { return &(*input); };
        RememberingEdgeIdUnique & operator++ () {
            value_type e = *input;
            ++input;
            if (!input.empty() && e == *input) {
                duplicates.insert(e);
                do {
                    ++input;
                } while (!input.empty() && *input == e);
            }

            return *this;
        };
        bool empty() const { return input.empty(); };
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
        _num_swaps_per_iteration(num_swaps_per_iteration)
    {}

    void run() {
        typename swap_vector::bufreader_type reader(_swaps);

        std::vector<swap_descriptor> currentSwaps;
        currentSwaps.reserve(_num_swaps_per_iteration);

        EdgeVectorCache edgeCache(_edges);

        typename debug_vector::bufwriter_type debug_vector_writer(_results);

        while (!reader.empty()) {
            currentSwaps.clear();

            // load edge endpoints for edges in the swap set
            stxxl::sorter<edgeid_t, GenericComparator<edgeid_t>::Ascending> swapSorter(GenericComparator<edgeid_t>::Ascending(), 128*IntScale::Mi);
            for (int_t i = 0; i < _num_swaps_per_iteration && !reader.empty(); ++i) {
                currentSwaps.emplace_back(*reader);
                swapSorter.push(currentSwaps.back().edges()[0]);
                swapSorter.push(currentSwaps.back().edges()[1]);
                ++reader;
            }

            std::cout << "Requesting " << swapSorter.size() << " non-unique edges for internal swaps" << std::endl;
            swapSorter.sort();

            RememberingEdgeIdUnique<decltype(swapSorter)> uniqueEdges(swapSorter);
            edgeCache.loadEdges(uniqueEdges);

            std::cout << "Identified " << uniqueEdges.duplicates.size() << " duplicate edge ids which could lead to conflicts" << std::endl;


            stx::btree_set<PossibleEdge> edgeSets;
            std::stack<PossibleEdge> newEdges;
            stxxl::sorter<edge_t, EdgeComparator> querySorter(EdgeComparator(), 128*IntScale::Mi);

            // construct possible conflict pairs
            for (auto s : currentSwaps) {
                auto eid0 = s.edges()[0];
                auto eid1 = s.edges()[1];

                bool store0 = (uniqueEdges.duplicates.find(eid0) != uniqueEdges.duplicates.end());
                bool store1 = (uniqueEdges.duplicates.find(eid1) != uniqueEdges.duplicates.end());

                bool direction = s.direction();

                auto addPossibleSwap = [&](edge_t e0, edge_t e1) {
                        edge_t t0, t1;
                        std::tie(t0, t1) = _swap_edges(e0, e1, direction);
                        if (t0.first != t0.second) {
                            if (store0) {
                                newEdges.push(PossibleEdge {eid0, t0});
                            } else {
                                querySorter.push(t0);
                            }
                        }

                        if (t1.first != t1.second) {
                            if (store1) {
                                newEdges.push(PossibleEdge {eid1, t1});
                            } else {
                                querySorter.push(t1);
                            }
                        }
                };

                auto e0 = edgeCache.getEdge(eid0);
                auto e1 = edgeCache.getEdge(eid1);
                addPossibleSwap(e0, e1);

                if (store1) {
                    for (auto it1 = edgeSets.lower_bound(PossibleEdge {eid1, std::make_pair(-1, -1)}); it1 != edgeSets.end() && it1->eid == eid1; ++it1) {
                        addPossibleSwap(e0, *it1);
                    }
                }

                if (store0) {
                    for (auto it0 = edgeSets.lower_bound(PossibleEdge {eid0, std::make_pair(-1, -1)}); it0 != edgeSets.end() && it0->eid == eid0; ++it0) {
                        addPossibleSwap(*it0, e1);
                        if (store1) {
                            for (auto it1 = edgeSets.lower_bound(PossibleEdge {eid1, std::make_pair(-1, -1)}); it1 != edgeSets.end() && it1->eid == eid1; ++it1) {
                                addPossibleSwap(*it0, *it1);
                            }
                        }
                    }
                }

                while (!newEdges.empty()) {
                    edgeSets.insert(newEdges.top());
                    newEdges.pop();
                }
            }

            std::cout << "Identified " << edgeSets.size() << " possible conflict edges among these edges" << std::endl;

            // construct query

            for (auto it = edgeSets.begin(); it != edgeSets.end(); ++it) {
                querySorter.push(*it);
            }

            // release memory of edge sets
            edgeSets = stx::btree_set<PossibleEdge>();

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

            std::cout << "Doing swaps" << std::endl;

            // do swaps
            for (const auto & s : currentSwaps) {
                auto eid0 = s.edges()[0];
                auto eid1 = s.edges()[1];

                auto& e0 = edgeCache.getEdge(eid0);
                auto& e1 = edgeCache.getEdge(eid1);

                SwapResult result;

                //std::cout << "Testing swap of " << e0.first << ", " << e0.second << " and " << e1.first << ", " << e1.second << std::endl;

                edge_t t0, t1;
                std::tie(t0, t1) = _swap_edges(e0, e1, s.direction());

                //std::cout << "Target edges " << t0.first << ", " << t0.second << " and " << t1.first << ", " << t1.second << std::endl;
                {
                    // compute whether swap can be performed and write debug info out
                    result.edges[0] = t0;
                    result.edges[1] = t1;
                    result.loop = (t0.first == t0.second || t1.first == t1.second); 

                    #ifdef NDEBUG
                    result.conflictDetected[0] = existsEdge.exists(t0);
                    result.conflictDetected[1] = existsEdge.exists(t1);
                    #else
                    result.conflictDetected[0] = existsEdge[t0];
                    result.conflictDetected[1] = existsEdge[t1]; 
                    #endif

                    result.performed = !result.loop && !(result.conflictDetected[0] || result.conflictDetected[1]);
                    result.normalize();

                    debug_vector_writer << result;
                }

                //std::cout << result << std::endl;

                if (result.loop) {
                    //std::cout << "Aborting swap, creating loop" << std::endl;
                    continue;
                } // loop
                assert(existsEdge.find(t0) != existsEdge.end() && existsEdge.find(t1) != existsEdge.end());

                if (!result.performed) {
                    //std::cout << "Found conflict" << std::endl;
                    continue; // conflict
                }

                #ifdef NDEBUG
                existsEdge.erase(e0);
                existsEdge.erase(e1);
                existsEdge.insert(t0);
                existsEdge.insert(t1);
                #else
                existsEdge[e0] = false;
                existsEdge[e1] = false;
                existsEdge[t0] = true;
                existsEdge[t1] = true;
                #endif

                // updates the values in edgeCache
                e0 = t0;
                e1 = t1;
                assert(edgeCache.getEdge(eid0) == t0);
                assert(edgeCache.getEdge(eid1) == t1);
            }

            edgeCache.flushEdges();

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
