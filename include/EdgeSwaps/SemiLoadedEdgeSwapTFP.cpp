#include <EdgeSwaps/SemiLoadedEdgeSwapTFP.h>
#include <stx/btree_map>

#include "EdgeVectorUpdateStream.h"

#include <Utils/AsyncStream.h>
#include <Utils/AsyncPusher.h>

#ifndef ASYNC_STREAMS
#define ASYNC_STREAMS
#endif

namespace EdgeSwapTFP {
    template<class EdgeReader>
    void SemiLoadedEdgeSwapTFP::_compute_dependency_chain_semi_loaded(EdgeReader & edge_reader_in, BoolStream & edge_remains_valid) {
        edge_remains_valid.clear();

        stx::btree_map<uint_t, uint_t> swaps_per_edges;
        uint_t swaps_per_edge = 1;

        #ifdef ASYNC_STREAMS
            AsyncStream<EdgeReader> edge_reader(edge_reader_in, false, 1.0e8);
            AsyncStream<EdgeSwapSorter> edge_swap_sorter(*_edge_swap_sorter, false, 1.0e8);
            AsyncStream<LoadedEdgeSwapSorter> loaded_edge_swap_sorter(*_loaded_edge_swap_sorter, false, 1.0e8);
            edge_reader.acquire();
            edge_swap_sorter.acquire();
            loaded_edge_swap_sorter.acquire();
        #else
            EdgeReader & edge_reader = edge_reader_in;
            EdgeSwapSorter & edge_swap_sorter = *_edge_swap_sorter;
            LoadedEdgeSwapSorter & loaded_edge_swap_sorter = *_loaded_edge_swap_sorter;
        #endif

        #ifdef ASYNC_PUSHERS
            AsyncPusher<DependencyChainEdgeSorter, DependencyChainEdgeMsg> depchain_edge_sorter(_depchain_edge_sorter, 1<<20, 20);
            AsyncPusher<DependencyChainSuccessorSorter, DependencyChainSuccessorMsg> depchain_successor_sorter(_depchain_successor_sorter);
        #else
            auto & depchain_edge_sorter = _depchain_edge_sorter;
            auto & depchain_successor_sorter = _depchain_successor_sorter;
        #endif

        // For every edge we send the incident vertices to the first swap,
        // i.e. the request with the lowest swap-id. We get this info by scanning
        // through the original edge list and the sorted request list in parallel
        // (i.e. by "merging" them). If there are multiple requests to an edge, we
        // send each predecessor the id of the next swap possibly affecting this edge.
        for (edgeid_t eid = 0; !edge_reader.empty(); ++eid, ++edge_reader) {
            edgeid_t requested_edge;
            swapid_t requesting_swap;
            const edge_t & edge = *edge_reader;

            auto match_request = [&]() {
                // indicate that a non-existing loaded edge is invalid
                while (!loaded_edge_swap_sorter.empty() && loaded_edge_swap_sorter->edge < edge) {
                    depchain_edge_sorter.push({loaded_edge_swap_sorter->swap_id, edge_t::invalid()}); 
                    ++loaded_edge_swap_sorter;
                }
                assert(loaded_edge_swap_sorter.empty() || loaded_edge_swap_sorter->edge >= edge);
                if (!edge_swap_sorter.empty() && edge_swap_sorter->edge_id == eid && !(!loaded_edge_swap_sorter.empty() && loaded_edge_swap_sorter->edge == edge && loaded_edge_swap_sorter->swap_id < edge_swap_sorter->swap_id)) {
                    requested_edge = edge_swap_sorter->edge_id;
                    requesting_swap = edge_swap_sorter->swap_id;
                    ++edge_swap_sorter;
                    return true;
                } else if (!loaded_edge_swap_sorter.empty() && loaded_edge_swap_sorter->edge == edge) {
                    requesting_swap = loaded_edge_swap_sorter->swap_id;
                    requested_edge = eid;
                    ++loaded_edge_swap_sorter;
                    return true;
                } else {
                    return false;
                }
            };

            if (match_request()) {
                edge_remains_valid.push(false);

                depchain_edge_sorter.push({requesting_swap, edge});

                if (compute_stats) {
                    swaps_per_edge = 1;
                }

                swapid_t last_swap = requesting_swap;

                while (match_request()) {
                    // if the edge is the same as the last edge, send an invalid edge and do not consider the swap in the dependency chain
                    if (UNLIKELY(requesting_swap%2 == 1 && last_swap + 1 == requesting_swap)) {
                        depchain_edge_sorter.push({requesting_swap, edge_t::invalid()});
                        requesting_swap = last_swap;
                    } else {
                        depchain_edge_sorter.push({requesting_swap, edge});
                        depchain_successor_sorter.push(DependencyChainSuccessorMsg{last_swap, requesting_swap});
                        DEBUG_MSG(_display_debug, "Report to swap " << last_swap << " that swap " << requesting_swap << " needs edge " << requested_edge);
                    }

                    if (compute_stats)
                        swaps_per_edge++;

                    last_swap = requesting_swap;
                }

                if (compute_stats)
                    swaps_per_edges[swaps_per_edge]++;

            } else {
                edge_remains_valid.push(true);
            }
        }

        assert(edge_swap_sorter.empty());
        assert(loaded_edge_swap_sorter.empty());
        assert(_loaded_edge_swap_sorter->empty());
        assert(_edge_swap_sorter->empty());

        #ifdef ASYNC_PUSHERS
            // indicate to async pushers that we are done
            depchain_edge_sorter.finish(false);
            depchain_successor_sorter.finish(false);
        #endif

        assert(edge_remains_valid.size() == _edges.size());

        if (compute_stats) {
            for (const auto &it : swaps_per_edges) {
                std::cout << it.first << " " << it.second << " #SWAPS-PER-EDGE-ID" << std::endl;
            }
        }


        edge_remains_valid.consume();

        #ifdef ASYNC_PUSHERS
            // wait for pushers until they are done
            depchain_edge_sorter.waitForPusher();
            depchain_successor_sorter.waitForPusher();

            #ifdef ASYNC_PUSHER_STATS
                std::cout << "Edge: ";
                depchain_edge_sorter.report_stats();
                std::cout << "Suc:  ";
                depchain_successor_sorter.report_stats();
            #endif
        #endif

        // then sort
        _depchain_successor_sorter.sort();
        _depchain_edge_sorter.sort();
    }

    void SemiLoadedEdgeSwapTFP::_start_processing(bool) {
        _loaded_edge_swap_sorter->sort();
        EdgeSwapTFP::_start_processing(false);
    }

    void SemiLoadedEdgeSwapTFP::_process_swaps() {
        constexpr bool show_stats = true;

        using UpdateStream = EdgeVectorUpdateStream<EdgeStream, BoolStream, decltype(_edge_update_sorter)>;

        if (!_edge_swap_sorter->size()) {
            // there are no swaps - let's see whether there are pending updates
            if ( _edge_update_sorter.size()) {
                UpdateStream update_stream(_edges, _last_edge_update_mask, _edge_update_sorter);
                update_stream.finish();
                _edges.rewind();
            }

            _edge_update_sorter.clear();

            _reset();
            _loaded_edge_swap_sorter->clear();

            return;
        }

        if (_first_run) {
            // first iteration
            _compute_dependency_chain_semi_loaded(_edges, _edge_update_mask);
            _edges.rewind();
            _first_run = false;

        } else {
            if (_edge_update_sorter_thread)
                _edge_update_sorter_thread->join();

            UpdateStream update_stream(_edges, _last_edge_update_mask, _edge_update_sorter);
            _compute_dependency_chain_semi_loaded(update_stream, _edge_update_mask);
            update_stream.finish();

            _edge_update_sorter.clear();
            _edges.rewind();

        }

#ifndef NDEBUG
        // test that input is lexicographically ordered and loop free
        {
            edge_t last_edge = *_edges;
            ++_edges;
            assert(!last_edge.is_loop());
            for(;!_edges.empty();++_edges) {
                auto & edge = *_edges;
                assert(!edge.is_loop());
                assert(last_edge < edge);
                last_edge = edge;
            }
            _edges.rewind();
        }
#endif

        std::swap(_edge_update_mask, _last_edge_update_mask);

        _report_stats("_compute_dependency_chain: ", show_stats);
        _compute_conflicts();
        _report_stats("_compute_conflicts: ", show_stats);
        _process_existence_requests();
        _report_stats("_process_existence_requests: ", show_stats);
        _perform_swaps();
        _report_stats("_perform_swaps: ", show_stats);

        _reset();

        // clear loaded edge swaps. the other sorter is cleared in _reset()
        _loaded_edge_swap_sorter->clear();

        if (_updated_edges_callback) {
            if (_edge_update_sorter_thread && _edge_update_sorter_thread->joinable())
                _edge_update_sorter_thread->join();

            _updated_edges_callback(_edge_update_sorter);
            _edge_update_sorter.rewind();
        }
    }
}
