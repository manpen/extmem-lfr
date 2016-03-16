#pragma once

#include <EdgeSwaps/EdgeSwapInternalSwapsBase.h>

#if 1
    #include <parallel/algorithm>
    #define SEQPAR __gnu_parallel
#else
    #define SEQPAR std
#endif

template <typename EdgeReader>
void EdgeSwapInternalSwapsBase::loadEdgeExistenceInformation(EdgeReader &edgeReader) {
    _edge_existence_pq.clear();
    _edge_existence_successors.clear();

    while (!_query_sorter.empty()) {
        if (edgeReader.empty() || *edgeReader >= _query_sorter->e) { // found edge or went past it (does not exist)
            // first request for the edge - give existence info if edge exists
            auto lastQuery = *_query_sorter;
            int_t numFound = 0;

            // found requested edge - advance reader
            while (!edgeReader.empty() && *edgeReader == _query_sorter->e) {
                ++numFound;
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
                _edge_existence_pq.push_back(edge_existence_answer_t {lastQuery.sid, lastQuery.e, numFound});
#else
                if (numFound > 0) {
                        _edge_existence_pq.push_back(edge_existence_answer_t {lastQuery.sid, lastQuery.e, numFound});
                    }
#endif
            }
        } else { // query edge might be after the current edge, advance edge reader to check
            ++edgeReader;
        }
    }

    _query_sorter.clear();
    SEQPAR::sort(_edge_existence_successors.begin(), _edge_existence_successors.end());
    std::make_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
}

template <typename EdgeReader>
void EdgeSwapInternalSwapsBase::executeSwaps(const std::vector< EdgeSwapBase::swap_descriptor > &swaps, std::vector< edge_t > &edges, const std::array<std::vector<bool>, 2>& swap_has_successor, EdgeReader &edgeReader) {
    bool show_stats = true;

    if (swaps.empty())
        return;

    _start_stats(show_stats);

    simulateSwapsAndGenerateEdgeExistenceQuery(swaps, edges, swap_has_successor);

    _report_stats("swap simulation", show_stats);

    std::cout << "Requesting " << _query_sorter.size() << " (possibly non-unique) possible conflict edges" << std::endl;

    loadEdgeExistenceInformation(edgeReader);

    _report_stats("load existence information", show_stats);

    std::cout << "Loaded " << _edge_existence_pq.size() << " existence values" << std::endl;
    std::cout << "Values might be forwarded " << _edge_existence_successors.size() << " times" << std::endl;

    std::cout << "Doing swaps" << std::endl;

    // do swaps
    performSwaps(swaps, edges);

    std::cout << "Capacity of internal edge existence PQ: " << _edge_existence_pq.capacity() << std::endl;

    _report_stats("perform swaps", show_stats);
}

