#include "EdgeSwapTFP.h"

#include <algorithm>
#include <array>
#include <vector>

#include <stx/btree_map>

#include "PQSorterMerger.h"
#include "EdgeVectorUpdateStream.h"

#include <Utils/AsyncStream.h>
#include <Utils/AsyncPusher.h>

#define ASYNC_STREAMS
//#define ASYNC_PUSHERS

namespace EdgeSwapTFP {
/*    void EdgeSwapTFP::push(const SwapDescriptor & swap) {
        // Every swap k to edges i, j sends one message (edge-id, swap-id) to each edge.
        // We then sort the messages lexicographically to gather all requests to an edge
        // at the same place.

        _edge_swap_sorter_pushing->push(EdgeSwapMsg(swap.edges()[0], _next_swap_id_pushing++));
        _edge_swap_sorter_pushing->push(EdgeSwapMsg(swap.edges()[1], _next_swap_id_pushing++));
        _swap_directions_pushing.push(swap.direction());

        if (UNLIKELY(_next_swap_id_pushing > 2*_run_length))
            _start_processing(false);
    } */

    template<class EdgeReader>
    void EdgeSwapTFP::_compute_dependency_chain(EdgeReader & edge_reader_in, BoolStream & edge_remains_valid) {
        edge_remains_valid.clear();

        edgeid_t eid = 0; // points to the next edge that can be read

        swapid_t last_swap = 0; // initialize to make gcc shut up

        stx::btree_map<uint_t, uint_t> swaps_per_edges;
        uint_t swaps_per_edge = 1;

        #ifdef ASYNC_STREAMS
            AsyncStream<EdgeReader> edge_reader(edge_reader_in, false, 1.0e8);
            AsyncStream<EdgeSwapSorter> edge_swap_sorter(*_edge_swap_sorter, false, 1.0e8);
            edge_reader.acquire();
            edge_swap_sorter.acquire();
        #else
            EdgeReader & edge_reader = edge_reader_in;
            EdgeSwapSorter & edge_swap_sorter = *_edge_swap_sorter;
        #endif

        #ifdef ASYNC_PUSHERS
            AsyncPusher<DependencyChainEdgeSorter, DependencyChainEdgeMsg> depchain_edge_sorter(_depchain_edge_sorter, 1<<20, 20);
            AsyncPusher<DependencyChainSuccessorSorter, DependencyChainSuccessorMsg> depchain_successor_sorter(_depchain_successor_sorter);
        #else
            auto & depchain_edge_sorter = _depchain_edge_sorter;
            auto & depchain_successor_sorter = _depchain_successor_sorter;
        #endif

        bool first_swap_of_edge = true;
        // For every edge we send the incident vertices to the first swap,
        // i.e. the request with the lowest swap-id. We get this info by scanning
        // through the original edge list and the sorted request list in parallel
        // (i.e. by "merging" them). If there are multiple requests to an edge, we
        // send each predecessor the id of the next swap possibly affecting this edge.
        for (; !edge_swap_sorter.empty(); ++edge_swap_sorter) {
            edgeid_t requested_edge;
            swapid_t requesting_swap;
            requested_edge = edge_swap_sorter->edge_id;
            requesting_swap = edge_swap_sorter->swap_id;

            // move reader buffer until we found the edge
            for (; eid < requested_edge; ++eid, ++edge_reader) {
                edge_remains_valid.push(first_swap_of_edge);
                first_swap_of_edge = true;
                assert(!edge_reader.empty());
            }

            const auto & edge = *edge_reader;

            // read edge and sent it to next node, if
            if (first_swap_of_edge) {
                assert(!edge_reader.empty());

                depchain_edge_sorter.push({requesting_swap, edge});

                first_swap_of_edge = false;

                if (compute_stats) {
                    swaps_per_edges[swaps_per_edge]++;
                    swaps_per_edge = 1;
                }

            } else {
                depchain_edge_sorter.push({requesting_swap, edge});
                depchain_successor_sorter.push(DependencyChainSuccessorMsg{last_swap, requesting_swap});
                DEBUG_MSG(_display_debug, "Report to swap " << last_swap << " that swap " << requesting_swap << " needs edge " << requested_edge);

                if (compute_stats)
                    swaps_per_edge++;
            }

            last_swap = requesting_swap;
        }

        #ifdef ASYNC_PUSHERS
            // indicate to async pushers that we are done
            depchain_edge_sorter.finish(false);
            depchain_successor_sorter.finish(false);
        #endif


        // fill validation stream
        edge_remains_valid.push(false); // last edge processed
        for(++eid; eid < static_cast<edgeid_t>(_edges.size()); ++eid)
            edge_remains_valid.push(true);

        assert(edge_remains_valid.size() == _edges.size());

        if (compute_stats) {
            swaps_per_edges[swaps_per_edge]++;

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

    /*
     * Since we do not yet know whether an swap can be performed, we keep for
     * every edge id a set of possible states. Initially this state is only the
     * edge as fetched in _compute_dependency_chain(), but after the first swap
     * the set contains at least two configurations, i.e. the original state
     * (in case the swap cannot be performed) and the swapped state.
     *
     * These configurations are kept in _dependency_chain_pq: Each swap receives
     * the complete state set of both edges and computes the cartesian product
     * of both. If there exists a successor swap (info stored in
     * _depchain_successor_sorter), i.e. a swap that will be  affect by the
     * current one, these information are forwarded.
     *
     * We further request information whether the edge exists by pushing requests
     * into _existence_request_sorter.
     */
    void EdgeSwapTFP::_compute_conflicts() {
        swapid_t sid = 0;

        // use pq in addition to _depchain_edge_sorter to pass messages between swaps
        assert(_dependency_chain_pq.empty());
        #ifdef ASYNC_STREAMS
             AsyncStream<DependencyChainEdgeSorter> depchain_edge_sorter(_depchain_edge_sorter, false);
             AsyncStream<DependencyChainSuccessorSorter> depchain_successor_sorter(_depchain_successor_sorter, false);
             depchain_edge_sorter.acquire();
             depchain_successor_sorter.acquire();
        #else
             auto & depchain_edge_sorter = _depchain_edge_sorter;
             auto & depchain_successor_sorter = _depchain_successor_sorter;
        #endif

        PQSorterMerger<DependencyChainEdgePQ, decltype(depchain_edge_sorter), compute_stats>
              depchain_pqsort(_dependency_chain_pq, depchain_edge_sorter);

        // statistics
        stx::btree_map<uint_t, uint_t> state_sizes;
        std::vector<edge_t> edges[2];

        uint_t max_elems_in_pq = 0;

        std::array<std::vector<edge_t>, 2> dd_new_edges;

        for (; !_swap_directions.empty(); ++_swap_directions, ++sid) {
            swapid_t successors[2];

            // fetch messages sent to this edge
            for (unsigned int i = 0; i < 2; i++) {
                edges[i].clear();

                // get successor
                if (!depchain_successor_sorter.empty()) {
                    auto &msg = *depchain_successor_sorter;


                    assert(msg.swap_id >= 2*sid+i);

                    if (msg.swap_id != 2*sid+i) {
                        successors[i] = 0;
                    } else {
                        DEBUG_MSG(_display_debug, "Got successor for S" << sid << ": " << msg);
                        successors[i] = msg.successor;
                        ++depchain_successor_sorter;
                    }

                } else {
                    successors[i] = 0;
                }


                // fetch possible edge state before swap
                while (!depchain_pqsort.empty()) {
                    const auto & msg = *depchain_pqsort;

                    assert(msg.swap_id >= 2*sid+i);
                    
                    if (msg.swap_id != 2*sid+i)
                        break;

                    edges[i].push_back(msg.edge);

                    // ensure that the first entry in edges is from sorter, so we do not have to resent it using PQ
                    if (UNLIKELY(depchain_pqsort.source() == SrcSorter && edges[i].size() != 1)) {
                        std::swap(edges[i].front(), edges[i].back());
                    }

                    ++depchain_pqsort;
                }

                DEBUG_MSG(_display_debug, "SWAP " << sid  << " Successor: " << successors[i] << " States: " << edges[i].size());

                // ensure that we received at least one state of the edge before the swap
                assert(!edges[i].empty());

                // ensure that dependent swap is in fact a successor (i.e. has larger index)
                assert(successors[i] == 0 || successors[i] > sid);
            }

            if (UNLIKELY(edges[0].front().is_invalid() || edges[1].front().is_invalid())) {
                for (unsigned char i = 0; i < 2; ++i) {
                    if (successors[i]) {
                        assert(!edges[i].front().is_invalid());

                        // do not send the first edge as it is already in the sorter
                        for (size_t j = 1; j < edges[i].size(); ++j) {
                            _dependency_chain_pq.push(DependencyChainEdgeMsg{successors[i], edges[i][j]});
                        }
                        depchain_pqsort.update();
                    }
                }
                continue;
            }

            // ensure that all messages to this swap have been consumed
            assert(depchain_pqsort.empty() || (*depchain_pqsort).swap_id > sid);


            #ifndef NDEBUG
                if (_display_debug) {
                    std::cout << "Swap " << sid << " edges[0] = [";
                    for (auto &e : edges[0]) std::cout << e << " ";
                    std::cout << "] edges[1]= [";
                    for (auto &e : edges[0]) std::cout << e << " ";
                    std::cout << "]" << std::endl;
                }
            #endif

            if (compute_stats) {
                state_sizes[edges[0].size() +edges[1].size()]++;
            }

            // compute "cartesian" product between possible edges to determine all possible new edges
            dd_new_edges[0].clear();
            dd_new_edges[1].clear();

            for (auto &e1 : edges[0]) {
                for (auto &e2 : edges[1]) {
                    edge_t new_edges[2];
                    std::tie(new_edges[0], new_edges[1]) = _swap_edges(e1, e2, *_swap_directions);

                    for (unsigned int i = 0; i < 2; i++) {
                        auto &new_edge = new_edges[i];
                        auto &queue = dd_new_edges[i];

                        queue.push_back(new_edge);

                        DEBUG_MSG(_display_debug, "Swap " << sid << " may yield " << new_edge);
                    }
                }
            }

            for (unsigned int i = 0; i < 2; i++) {
                auto & dd = dd_new_edges[i];

                // sort to support binary search and linear time deduplication
                if (UNLIKELY(dd_new_edges[i].size() > 1))
                    std::sort(dd.begin(), dd.end());

                // send the hard cases (target edges)
                {
                    edge_t send_edge = edge_t::invalid();
                    for (auto it = dd.cbegin(); it != dd.cend(); ++it) {
                        if (UNLIKELY(send_edge == *it))
                            continue;

                        send_edge = *it;

                        //if (UNLIKELY(send_edge == edges[i].front()))
                        //    continue;

                        if (UNLIKELY(successors[i])) {
                            _dependency_chain_pq.push(DependencyChainEdgeMsg{successors[i], send_edge});
                        }

                        _existence_request_sorter.push(ExistenceRequestMsg{send_edge, sid, false});
                    }
                }

                // forward only (source edge)
                for (const auto & edge : edges[i]) {
                    // check whether already sent above
                    if (UNLIKELY(std::binary_search(dd.cbegin(), dd.cend(), edge)))
                        continue;

                    if (UNLIKELY(successors[i] && edge != edges[i].front())) {
                        _dependency_chain_pq.push(DependencyChainEdgeMsg{successors[i], edge});
                    }

                    _existence_request_sorter.push(ExistenceRequestMsg{edge, sid, true});
                }
            }

            // if we pushed something into the PQ we need to update the merger
            if (UNLIKELY(successors[0] || successors[1])) {
                depchain_pqsort.update();
                if (compute_stats)
                    max_elems_in_pq = std::max<uint_t>(_dependency_chain_pq.size(), max_elems_in_pq);
            }
        }

        std::cout << "Elements remaining in PQ: " << _dependency_chain_pq.size() << std::endl;

        if (compute_stats) {
            for (const auto &it : state_sizes) {
                std::cout << it.first << " " << it.second << " #STATE-SIZE" <<
                std::endl;
            }
            std::cout << "Max elements in PQ: " << max_elems_in_pq << std::endl;
            depchain_pqsort.dump_stats();
        }

        _existence_request_sorter.sort();
        _swap_directions.rewind();

        if (_async_processing) {
            _depchain_thread.reset(new std::thread([&]() {
                _depchain_successor_sorter.rewind();
                _depchain_edge_sorter.rewind();
            }));
        } else {
            _depchain_successor_sorter.rewind();
            _depchain_edge_sorter.rewind();
        }
    }

    /*
     * We parallel stream through _edges and _existence_request_sorter#
     * to check whether a requested edge exists in the input graph.
     * The result is sent to the first swap requesting using
     * _existence_info_pq. We additionally compute a dependency chain
     * by informing every swap about the next one requesting the info.
     */
    void EdgeSwapTFP::_process_existence_requests() {

        uint64_t stat_exist_reqs = _existence_request_sorter.size();
        uint64_t stat_forward_only = 0;
        uint64_t stat_dropped_dep = 0;
        stx::btree_map<uint_t, swapid_t> stat_hist_requests_per_edge;
        
        #ifdef ASYNC_STREAMS
            AsyncStream<ExistenceRequestSorter> existence_request_sorter(_existence_request_sorter);
        #else
            auto & existence_request_sorter = _existence_request_sorter;
        #endif



        while (!existence_request_sorter.empty()) {
            auto &request = *existence_request_sorter;
            edge_t current_edge = request.edge;

            // find edge in graph
            bool exists = false;
            for (; !_edges.empty(); ++_edges) {
                const auto &edge = *_edges;
                if (edge > current_edge) break;
                exists = (edge == current_edge);
            }

            // build dependency chain (i.e. inform earlier swaps about later ones) and find the earliest swap
            swapid_t last_swap = request.swap_id();
            bool foundTargetEdge = false; // if we already found a swap where the edge is a target

            swapid_t stat_requests_per_edge = 0;

            // observe that existence requests are ordered desc so we iterate over a dependency chain backwards
            for (; !existence_request_sorter.empty(); ++existence_request_sorter) {
                auto &request = *existence_request_sorter;
                if (request.edge != current_edge)
                    break;

                if (UNLIKELY(last_swap != request.swap_id() && foundTargetEdge)) {
                    // inform an earlier swap about later swaps that need the new state
                    assert(last_swap > request.swap_id());
                    _existence_successor_sorter.push(ExistenceSuccessorMsg{request.swap_id(), current_edge, last_swap});
                    DEBUG_MSG(_display_debug, "Inform swap " << request.swap_id() << " that " << last_swap << " is a successor for edge " << current_edge);
                } else if (compute_stats) {
                    stat_dropped_dep++;
                }

                last_swap = request.swap_id();
                foundTargetEdge = (foundTargetEdge || !request.forward_only());

                if (compute_stats) {
                    stat_forward_only += request.forward_only();
                    stat_requests_per_edge++;
                }
            }

            if (compute_stats)
                stat_hist_requests_per_edge[stat_requests_per_edge]++;

            // inform earliest swap whether edge exists
            if (foundTargetEdge) {
            #ifdef NDEBUG
                if (exists) {
                    _existence_info_sorter.push(ExistenceInfoMsg{last_swap, current_edge});
                }
            #else
                _existence_info_sorter.push(ExistenceInfoMsg{last_swap, current_edge, exists});
                DEBUG_MSG(_display_debug, "Inform swap " << last_swap << " edge " << current_edge << " exists " << exists);
            #endif
            }
        }

        if (compute_stats) {
            std::cout << "Existence requests: " << stat_exist_reqs << "\n"
                         "Forward only requests: " << stat_forward_only << "\n"
                         "Dep. Chains shortend: " << stat_dropped_dep << std::endl;

            for (const auto &it : stat_hist_requests_per_edge) {
                std::cout << it.first << " " << it.second << " #EXIST-REQ-PER-EDGE" << std::endl;
            }
        }

        if (_async_processing) {
            std::thread t1([&](){_existence_successor_sorter.sort();});
            std::thread t2([&](){_existence_info_sorter.sort();});
            //_existence_request_sorter.finish_clear();
            t1.join(); t2.join();
        } else {
            //_existence_request_sorter.finish_clear();
            _existence_successor_sorter.sort();
            _existence_info_sorter.sort();
        }

        _edges.rewind();
    }

    /*
     * Information sources:
     *  _swaps contains definition of swaps
     *  _depchain_successor_sorter stores swaps we need to inform about our actions
     */
    void EdgeSwapTFP::_perform_swaps() {
        if (_depchain_thread) _depchain_thread->join();

#ifdef EDGE_SWAP_DEBUG_VECTOR
        // debug only
        debug_vector::bufwriter_type debug_vector_writer(_result);
#endif

       #ifdef ASYNC_STREAMS
             AsyncStream<DependencyChainEdgeSorter> depchain_edge_sorter(_depchain_edge_sorter, false);
             AsyncStream<DependencyChainSuccessorSorter> depchain_successor_sorter(_depchain_successor_sorter, false);
             AsyncStream<ExistenceInfoSorter> existence_info_sorter(_existence_info_sorter, false);
             depchain_edge_sorter.acquire();
             depchain_successor_sorter.acquire();
             existence_info_sorter.acquire();
        #else
             auto & depchain_edge_sorter = _depchain_edge_sorter;
             auto & depchain_successor_sorter = _depchain_successor_sorter;
             auto & existence_info_sorter = _existence_info_sorter;
        #endif       

        assert(_dependency_chain_pq.empty());
        PQSorterMerger<DependencyChainEdgePQ, decltype(depchain_edge_sorter)> edge_state_pqsort(_dependency_chain_pq, depchain_edge_sorter);

        assert(_existence_info_pq.empty());
        PQSorterMerger<ExistenceInfoPQ, decltype(existence_info_sorter)> existence_info_pqsort(_existence_info_pq, existence_info_sorter);

        swapid_t sid = 0;

        std::vector<edge_t> existence_infos;
        #ifndef NDEBUG
            std::vector<edge_t> missing_infos;
        #endif

        swapid_t counter_performed = 0;
        swapid_t counter_not_performed = 0;
        swapid_t counter_loop = 0;

        for (; !_swap_directions.empty(); ++_swap_directions, ++sid) {
            edge_state_pqsort.update();

            // collect the current state of the edge to be swapped
            edge_t edges[4];
            edge_t *new_edges = edges + 2;
            bool edge_prev_updated[2] = {false, false};

            for (unsigned int i = 0; i < 2; i++) {
                assert(!edge_state_pqsort.empty());

                // if the
                do {
                    const auto & msg = *edge_state_pqsort;
                    assert(msg.swap_id == 2*sid+i);
                    assert(edge_state_pqsort.source() == SrcSorter || !edge_prev_updated[i]);

                    if (!edge_prev_updated[i] || edge_state_pqsort.source() == SrcPriorityQueue)
                        edges[i] = msg.edge;

                    edge_prev_updated[i] = (edge_prev_updated[i] || (edge_state_pqsort.source() == SrcPriorityQueue));

                    ++edge_state_pqsort;
                } while(UNLIKELY(
                    !edge_state_pqsort.empty() &&
                    (*edge_state_pqsort).swap_id == 2*sid+i
                ));
            }

            // in the case of invalid swaps (i.e. one edge is invalid) do perform forwarding for the other edge! Note that we did not produce any existence requests, so we don't need to care about them.
            bool edge_invalid = false;
            if (UNLIKELY(edges[0].is_invalid() || edges[1].is_invalid())) {
                new_edges[0] = edges[0];
                new_edges[1] = edges[1];
                edge_invalid = true;
            } else {
                // compute swapped edges
                std::tie(new_edges[0], new_edges[1]) = _swap_edges(edges[0], edges[1], *_swap_directions);
            }

            #ifndef NDEBUG
                if (_display_debug) {
                    std::cout << "State in " << sid << ": ";
                    for (unsigned int i = 0; i < 4; i++) {
                        std::cout << edges[i] << " ";
                    }
                    std::cout << std::endl;
                }
            #endif

            // gather all edge states that have been sent to this swap
            {
                existence_info_pqsort.update();
                for (; !existence_info_pqsort.empty() && (*existence_info_pqsort).swap_id == sid; ++existence_info_pqsort) {
                    const auto &msg = *existence_info_pqsort;

                    #ifdef NDEBUG
                        existence_infos.push_back(msg.edge);
                    #else
                        if (msg.exists) {
                            existence_infos.push_back(msg.edge);
                        } else {
                            missing_infos.push_back(msg.edge);
                        }
                    #endif
                }
                // when an edge is invalid, there should be no existence information
                assert(!edge_invalid || (existence_infos.empty() && missing_infos.empty()));
            }

            #ifndef NDEBUG
                if (_display_debug) {
                    for (auto &k : existence_infos)
                        std::cout << sid << " " << k << " exists" << std::endl;
                    for (auto &k : missing_infos)
                        std::cout << sid << " " << k << " is missing" << std::endl;
                }
            #endif

            // check if there's an conflicting edge
            bool conflict_exists[2];
            for (unsigned int i = 0; i < 2; i++) {
                bool exists = std::binary_search(existence_infos.begin(), existence_infos.end(), new_edges[i]);
                #ifndef NDEBUG
                    if (!exists) {
                        assert(std::binary_search(missing_infos.begin(), missing_infos.end(), new_edges[i]));
                    }
                #endif
                conflict_exists[i] = exists;
            }

            // can we perform the swap?
            const bool loop = new_edges[0].is_loop() || new_edges[1].is_loop();
            const bool perform_swap = !(conflict_exists[0] || conflict_exists[1] || loop);

            if (compute_stats) {
                counter_performed += perform_swap;
                counter_not_performed += !perform_swap;
                counter_loop += loop;
            }

            // write out debug message if the swap is not invalid
            if (produce_debug_vector && !edge_invalid) {
                SwapResult res;
                res.performed = perform_swap;
                res.loop = loop;
                std::copy(new_edges, new_edges + 2, res.edges);
                for(unsigned int i=0; i < 2; i++) {
                    res.edges[i] = new_edges[i];
                    res.conflictDetected[i] = conflict_exists[i];
                }
                res.normalize();

#ifdef EDGE_SWAP_DEBUG_VECTOR
                debug_vector_writer << res;
#endif
                DEBUG_MSG(_display_debug, "Swap " << sid << " " << res);
            }

            // forward edge state to successor swap
            bool successor_found[2] = {false, false};
            for (; !depchain_successor_sorter.empty(); ++depchain_successor_sorter) {
                auto &succ = *depchain_successor_sorter;

                assert(succ.swap_id >= 2*sid);
                if (succ.swap_id > 2*sid+1)
                    break;

                assert(succ.successor > 2*sid+1);

                const int successor = succ.swap_id&1;
                if (perform_swap || edge_prev_updated[successor]) {
                    _dependency_chain_pq.push(DependencyChainEdgeMsg{succ.successor, edges[successor + 2*perform_swap]});
                }

                successor_found[successor] = true;
            }

            // send current state of edge iff there are no successors to this edge
            for(unsigned int i=0; i<2; i++) {
                // only forward valid edges!
                if (!successor_found[i] && LIKELY(!edges[i + 2 * perform_swap].is_invalid())) {
                    _edge_update_sorter.push(edges[i + 2*perform_swap]);
                }
            }

            // forward existence information
            for (; !_existence_successor_sorter.empty(); ++_existence_successor_sorter) {
                auto &succ = *_existence_successor_sorter;

                assert(succ.swap_id >= sid);
                if (succ.swap_id > sid) break;

                if ((perform_swap && (succ.edge == new_edges[0] || succ.edge == new_edges[1])) ||
                    (!perform_swap && (succ.edge == edges[0] || succ.edge == edges[1]))) {
                    // target edges always exist (or source if no swap has been performed)
                    #ifdef NDEBUG
                        _existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge});
                    #else
                        _existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge, true});
                        DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << true << " to " << succ.successor);
                    #endif
                } else if (succ.edge == edges[0] || succ.edge == edges[1]) {
                    // source edges never exist (if no swap has been performed, this has been handled above)
                    #ifndef NDEBUG
                        _existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge, false});
                        DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << false << " to " << succ.successor);
                    #endif
                } else {
                    #ifdef NDEBUG
                        if (std::binary_search(existence_infos.begin(), existence_infos.end(), succ.edge)) {
                            _existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge});
                        }
                    #else
                    bool exists = std::binary_search(existence_infos.begin(), existence_infos.end(), succ.edge);
                    _existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge, exists});
                    if (!exists) {
                        assert(std::binary_search(missing_infos.begin(), missing_infos.end(), succ.edge));
                    }
                    DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << exists << " to " << succ.successor);
                    #endif
                }
            }

            existence_infos.clear();
            #ifndef NDEBUG
            missing_infos.clear();
            #endif

        }

        if (compute_stats) {
            std::cout << "Swaps performed: " << counter_performed
            << " not performed: " << counter_not_performed
            << " due to loops: " << counter_loop << std::endl;
        }

        if (_result_thread) _result_thread->join();
#ifdef EDGE_SWAP_DEBUG_VECTOR
        if (_async_processing) {
            _result_thread.reset(
                new std::thread([&](){debug_vector_writer.finish();})
            );
        } else {
            debug_vector_writer.finish();
        }
#endif

        // check message data structures are empty
        assert(_depchain_successor_sorter.empty());
        //_depchain_successor_sorter.finish_clear();

        assert(_existence_successor_sorter.empty());
        //_existence_successor_sorter.finish_clear();

        assert(_existence_info_pq.empty());
        //_existence_info_sorter.finish_clear();

        if (_async_processing) {
            _edge_update_sorter_thread.reset(
                new std::thread([&](){_edge_update_sorter.sort();})
            );
        } else {
            _edge_update_sorter.sort();
        }
    }

    void EdgeSwapTFP::_process_swaps() {
        constexpr bool show_stats = true;

        using UpdateStream = EdgeVectorUpdateStream<EdgeStream, BoolStream, decltype(_edge_update_sorter)>;

        if (!_edge_swap_sorter->size()) {
            // there are no swaps - let's see whether there are pending updates
            if (_edge_update_sorter.size()) {
                UpdateStream update_stream(_edges, _last_edge_update_mask, _edge_update_sorter);
                update_stream.finish();
                _edges.rewind();
            }
            _edge_update_sorter.clear();

            _reset();

            return;
        }

        if (_first_run) {
            // first iteration
            _compute_dependency_chain(_edges, _edge_update_mask);
            _edges.rewind();
            _first_run = false;

        } else {
            if (_edge_update_sorter_thread)
                _edge_update_sorter_thread->join();

            UpdateStream update_stream(_edges, _last_edge_update_mask, _edge_update_sorter);
            _compute_dependency_chain(update_stream, _edge_update_mask);
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
    }


    void EdgeSwapTFP::_start_processing(bool async) {
        // prepare new structures
        _edge_swap_sorter_pushing->sort();
        _swap_directions_pushing.consume();

        // wait for compution to finish (if there is some)
        if (_process_thread.joinable())
            _process_thread.join();

        // reset old data structures
        _swap_directions.clear();
        _next_swap_id_pushing = 0;

        // swap staging and processing area
        std::swap(_edge_swap_sorter_pushing, _edge_swap_sorter);
        std::swap(_swap_directions_pushing, _swap_directions);

        if (async) {
            // start worker thread
            _process_thread = std::thread(&EdgeSwapTFP::_process_swaps, this);
        } else {
            // do it ourselves
            _process_swaps();
        }
    }

    void EdgeSwapTFP::run() {
        _start_processing();
        _start_processing(false);
        _first_run = true;
    }
};
