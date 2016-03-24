#include "EdgeSwapParallelTFP.h"

#include <algorithm>
#include <array>
#include <vector>

#include <stx/btree_map>
#include <stxxl/priority_queue>

#include "PQSorterMerger.h"
#include "EdgeVectorUpdateStream.h"

namespace EdgeSwapParallelTFP {
    void EdgeSwapParallelTFP::process_swaps() {
        // if we have no swaps to load and no edges to write back, do nothing (might happen by calling process_swaps several times)
        if (_edge_swap_sorter.empty() && _used_edge_ids.empty()) return;

        int old_num_threads = _num_threads;

        #pragma omp parallel num_threads(old_num_threads)
        #pragma omp single
        {
            _num_threads = omp_get_num_threads();
        }

        if (_num_threads < old_num_threads) {
            STXXL_ERRMSG("Warning: Did only get " << _num_threads << " threads in the parallel section instead of the requested " << old_num_threads << " threads.");
        }

        std::vector<std::unique_ptr<DependencyChainEdgeSorter>> swap_edge_sorter(_num_threads);
        std::vector<std::unique_ptr<DependencyChainSuccessorSorter>> swap_edge_dependencies_sorter(_num_threads);

        // allocate sorters only if there is actually something to do!
        if (!_edge_swap_sorter.empty()) {
            for (auto &s : swap_edge_sorter) {
                s.reset(new DependencyChainEdgeSorter(DependencyChainEdgeComparatorSorter(), _sorter_mem/_num_threads));
            }
            for (auto &s : swap_edge_dependencies_sorter) {
                s.reset(new DependencyChainSuccessorSorter(DependencyChainSuccessorComparator(), _sorter_mem/_num_threads));
            }
        }

        _load_and_update_edges(swap_edge_sorter, swap_edge_dependencies_sorter);

        if (_swap_id > 0) {
            for (auto &w : _swap_direction_writer) {
                w->finish();
            }

            for (int i = 0; i < _num_threads; ++i) {
                _swap_direction_writer[i].reset(new BoolVector::bufwriter_type(*_swap_direction[i]));
            }
        }

        // re-initialize data structures for new swaps
        _swap_id = 0;
        _edge_swap_sorter.clear();
        _num_threads = old_num_threads;

    }

    void EdgeSwapParallelTFP::_load_and_update_edges(std::vector<std::unique_ptr<DependencyChainEdgeSorter>> &edge_output, std::vector<std::unique_ptr<DependencyChainSuccessorSorter>> &dependency_output) {
        uint64_t numSwaps = _swap_id;

        // copy old edge ids for writing back
        EdgeIdVector old_edge_ids;
        old_edge_ids.swap(_used_edge_ids);
        _used_edge_ids.reserve(_swap_id * 2);
        EdgeIdVector::bufwriter_type edge_id_writer(_used_edge_ids);

        _edge_swap_sorter.sort();


        if (compute_stats) {
            std::cout << "Requesting " << _edge_swap_sorter.size() << " non-unique edges for internal swaps" << std::endl;
        }


        { // load edges from EM. Generates successor information and swap_edges information (for the first edge in the chain).
            auto use_edge = [&] (const edge_t & cur_e, edgeid_t id) {
                swapid_t sid;
                unsigned char spos;
                int tid;

                auto match_request = [&]() {
                    if (!_edge_swap_sorter.empty() && _edge_swap_sorter->eid == id) {
                        sid = _edge_swap_sorter->sid;
                        spos = _edge_swap_sorter->spos;
                        tid = _thread(sid);
                        return true;
                    } else {
                        return false;
                    }
                };

                if (match_request()) {
                    assert(edge_output[tid]);
                    assert(dependency_output[tid]);
                    edge_id_writer << id;
                    edge_output[tid]->push(DependencyChainEdgeMsg {sid, spos, cur_e});

                    auto lastSpos = spos;
                    auto lastSid = sid;
                    auto lastTid = tid;

                    // further requests for the same swap - store successor information
                    while (match_request()) {
                        // set edge id to internal edge id
                        assert(dependency_output[lastTid]);
                        dependency_output[lastTid]->push(DependencyChainSuccessorMsg {lastSid, lastSpos, sid, spos});
                        lastSpos = spos;
                        lastSid = sid;
                    }
                }
            };

            edgeid_t id = 0;
            typename edge_vector::bufreader_type edge_reader(_edges);

            if (old_edge_ids.empty()) {
                // just read edges
                for (; !edge_reader.empty(); ++id, ++edge_reader) {
                    use_edge(*edge_reader);
                }
            } else {
                // read old edge vector and merge in updates, write out result
                edge_vector output_vector;
                output_vector.reserve(_edges.size());
                typename edge_vector::bufwriter_type writer(output_vector);

                EdgeIdVector::bufreader_type old_e(old_edge_ids);
                int_t read_id = 0;
                edge_t cur_e;

                for (; !edge_reader.empty() || !_edge_update_sorter.empty(); ++id) {
                    // Skip old edges
                    while (!old_e.empty() && *old_e == read_id) {
                        ++edge_reader;
                        ++read_id;
                        ++old_e;
                    }

                    // merge update edges and read edges
                    if (!_edge_update_sorter.empty() && (edge_reader.empty() || *_edge_update_sorter < *edge_reader)) {
                        cur_e = *_edge_update_sorter;
                        writer << cur_e;
                        ++_edge_update_sorter;
                    } else {
                        if (edge_reader.empty()) { // due to the previous while loop both could be empty now
                            break; // abort the loop as we do not have any edges to process anymore.
                        }

                        cur_e = *edge_reader;
                        writer << cur_e;
                        ++read_id;
                        ++edge_reader;
                    }

                    use_edge(cur_e);
                }

                writer.finish();
                _edges.swap(output_vector);

                _edge_update_sorter.finish_clear();
            }
        }


        if (numSwaps > 0) {
            for (auto &s : edge_output) {
                s->sort();
            }

            for (auto &s : dependency_output) {
                s->sort();
            }

            _edge_swap_sorter.finish_clear();
        }
    }

    /*
     * Since we do not yet know whether an swap can be performed, we keep for
     * every edge id a set of possible states. Initially this state is only the
     * edge as fetched in _compute_dependency_chain(), but after the first swap
     * the set contains at least two configurations, i.e. the original state
     * (in case the swap cannot be performed) and the swapped state.
     *
     * These configurations are kept in depchain_edge_pq: Each swap receives
     * the complete state set of both edges and computes the cartesian product
     * of both. If there exists a successor swap (info stored in
     * _depchain_successor_sorter), i.e. a swap that will be  affect by the
     * current one, these information are forwarded.
     *
     * We further request information whether the edge exists by pushing requests
     * into _existence_request_sorter.
     */
    void EdgeSwapParallelTFP::_compute_conflicts(std::vector< std::unique_ptr< EdgeSwapParallelTFP::DependencyChainEdgeSorter > > &swap_edges, std::vector< std::unique_ptr< EdgeSwapParallelTFP::DependencyChainSuccessorSorter > > &dependencies, ExistenceRequestMerger &requestOutputMerger) {
        swapid_t sid = 0;
        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;
        using DependencyChainEdgePQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<DependencyChainEdgeMsg, DependencyChainEdgeComparatorPQ, _pq_mem, 1 << 20>::result;
        using DependencyChainEdgePQBlock = typename DependencyChainEdgePQ::block_type;

        uint_t batch_size_per_thread = 1*UIntScale::Mi;

        struct edge_information_t {
            std::array<int_t, 2> is_set;
            std::array<std::vector<edge_t>, 2> edges;
        };

        std::vector<std::vector<edge_information_t>> edge_information(_num_threads, std::vector<edge_information_t>(batch_size_per_thread));

        stxxl::stream::basic_runs_creator<stxxl::stream::from_sorted_sequences<ExistenceRequestMsg>,
        ExistenceRequestComparator, STXXL_DEFAULT_BLOCK_SIZE(ExistenceRequestMsg), STXXL_DEFAULT_ALLOC_STRATEGY> existence_request_runs_creator (ExistenceRequestComparator(), SORTER_MEM);

        #pragma omp parallel
        {

            int tid = omp_get_thread_num();

            // use pq in addition to _depchain_edge_sorter to pass messages between swaps
            stxxl::read_write_pool<DependencyChainEdgePQBlock>
                pq_pool(_pq_pool_mem / 2 / DependencyChainEdgePQBlock::raw_size,
                        _pq_pool_mem / 2 / DependencyChainEdgePQBlock::raw_size);
            DependencyChainEdgePQ depchain_edge_pq(pq_pool);
            PQSorterMerger<DependencyChainEdgePQ, DependencyChainEdgeSorter, compute_stats>
                depchain_pqsort(depchain_edge_pq, *swap_edges[tid]);

            BoolVector::bufreader_type direction_reader(_swap_direction[tid]);

            auto &dep = *dependencies[tid];

            std::vector<ExistenceRequestMsg> existence_request_buffer;
            existence_request_buffer.reserve(SORTER_MEM/_num_threads);
            const size_t max_existence_request_buffer_size = SORTER_MEM / _num_threads / sizeof(ExistenceRequestMsg);

            swapid_t sid = tid;
            while  (sid < _swap_id) {
                for (int_t i = 0; i < batch_size_per_thread && sid < _swap_id; ++i, sid += _num_threads) {
                    std::array<std::vector<edge_t>, 2> dd_new_edges;

                    swapid_t successors[2];

                    assert(!direction_reader.empty());

                    bool direction = *direction_reader;
                    ++direction_reader;

                    auto & edges = edge_information[tid][i].edges;

                    // fetch messages sent to this edge
                    for (unsigned int spos = 0; spos < 2; spos++) {
                        // get successor
                        if (!dep.empty()) {
                            auto &msg = *dep;

                            assert(msg.swap_id >= sid);
                            assert(msg.swap_id > sid || msg.edge_id >= eid);

                            if (msg.swap_id != sid || msg.spos != spos) {
                                successors[i] = 0;
                            } else {
                                DEBUG_MSG(_display_debug, "Got successor for S" << sid << ", E" << eid << ": " << msg);
                                successors[i] = msg.successor;
                                ++dep;
                            }
                        } else {
                            successors[i] = 0;
                        }


                        // fetch possible edge state before swap
                        while (!edge_information[tid][i].is_set[spos] && !depchain_pqsort.empty()) {
                            const auto & msg = *depchain_pqsort;

                            if (msg.swap_id != sid || msg.spos != spos)
                                break;

                            edges[i].push_back(msg.edge);

                            /* // this optimization isn't possible in the parallel case as otherwise we cannot find out anymore if we got a message or not.
                            // ensure that the first entry in edges is from sorter, so we do not have to resent it using PQ
                            if (UNLIKELY(depchain_pqsort.source() == SrcSorter && edges[i].size() != 1)) {
                                std::swap(edges[i].front(), edges[i].back());
                            }
                            */

                            ++depchain_pqsort;
                        }

                        DEBUG_MSG(_display_debug, "SWAP " << sid << " Edge " << eid << " Successor: " << successors[i] << " States: " << edges[i].size());

                        // ensure that we received at least one state of the edge before the swap
                        if (edges[i].empty()) {
                            while (!edge_information[tid][i].is_set[spos]) { // busy waiting for other thread to supply information
                            }
                        }
                        assert(!edges[i].empty());

                        // ensure that dependent swap is in fact a successor (i.e. has larger index)
                        assert(successors[i] == 0 || successors[i] > sid);
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

                    // compute "cartesian" product between possible edges to determine all possible new edges
                    dd_new_edges[0].clear();
                    dd_new_edges[1].clear();

                    for (auto &e1 : edges[0]) {
                        for (auto &e2 : edges[1]) {
                            edge_t new_edges[2];
                            std::tie(new_edges[0], new_edges[1]) = _swap_edges(e1, e2, direction);

                            for (unsigned int i = 0; i < 2; i++) {
                                auto &new_edge = new_edges[i];
                                auto &queue = dd_new_edges[i];

                                queue.push_back(new_edge);

                                DEBUG_MSG(_display_debug, "Swap " << sid << " may yield " << new_edge << " at " << i);
                            }
                        }
                    }

                    for (unsigned int i = 0; i < 2; i++) {
                        auto & dd = dd_new_edges[i];
                        const auto & edgeid = swap.edges()[i];

                        // sort to support binary search and linear time deduplication
                        if (dd_new_edges[i].size() > 1)
                            std::sort(dd.begin(), dd.end());

                        // send the hard cases (target edges)
                        {
                            edge_t send_edge = edge_t(-1, -1);
                            for (auto it = dd.cbegin(); it != dd.cend(); ++it) {
                                if (UNLIKELY(send_edge == *it))
                                    continue;

                                send_edge = *it;

                                //if (UNLIKELY(send_edge == edges[i].front()))
                                //    continue;

                                if (UNLIKELY(successors[i])) {
                                // FIXME use (new) local buffer or write directly if in current batch
                                    depchain_edge_pq.push(DependencyChainEdgeMsg{successors[i], edgeid, send_edge});
                                }

                                // FIXME use buffer
                                _existence_request_sorter.push(ExistenceRequestMsg{send_edge, sid, false});
                            }
                        }

                        // forward only (source edge)
                        for (const auto & edge : edges[i]) {
                            // check whether already sent above
                            if (UNLIKELY(std::binary_search(dd.cbegin(), dd.cend(), edge)))
                                continue;

                            if (UNLIKELY(successors[i] && edge != edges[i].front())) {
                                // FIXME use (new) local buffer or write directly if in current batch
                                depchain_edge_pq.push(DependencyChainEdgeMsg{successors[i], edgeid, edge});
                            }

                            // FIXME use buffer
                            _existence_request_sorter.push(ExistenceRequestMsg{edge, sid, true});
                        }
                    }

                    // if we pushed something into the PQ we need to update the merger
                    if (UNLIKELY(successors[0] || successors[1])) {
                        depchain_pqsort.update();
                    }
                }

                // finished batch.
                // TODO: distribute and insert buffer for depchain_edge_pq

                // TODO: possibly sort edge existence requests - collect sizes and decide then if it makes sense to sort (all of them?)
            } // finished processing all swaps
        } // end of parallel section

        requestOutputMerger.initialize(existence_request_runs_creator.result());
    }

    /*
     * We parallel stream through _edges and _existence_request_sorter#
     * to check whether a requested edge exists in the input graph.
     * The result is sent to the first swap requesting using
     * _existence_info_pq. We additionally compute a dependency chain
     * by informing every swap about the next one requesting the info.
     */
    void EdgeSwapParallelTFP::_process_existence_requests() {
        typename edge_vector::bufreader_type edge_reader(_edges);

        while (!_existence_request_sorter.empty()) {
            auto &request = *_existence_request_sorter;
            edge_t current_edge = request.edge;

            // find edge in graph
            bool exists = false;
            for (; !edge_reader.empty(); ++edge_reader) {
                const auto &edge = *edge_reader;
                if (edge > current_edge) break;
                exists = (edge == current_edge);
            }

            // build depencency chain (i.e. inform earlier swaps about later ones) and find the earliest swap
            swapid_t last_swap = request.swap_id;
            bool foundTargetEdge = false; // if we already found a swap where the edge is a target
            for (; !_existence_request_sorter.empty(); ++_existence_request_sorter) {
                auto &request = *_existence_request_sorter;
                if (request.edge != current_edge)
                    break;

                if (last_swap != request.swap_id && foundTargetEdge) {
                    // inform an earlier swap about later swaps that need the new state
                    assert(last_swap > request.swap_id);
                    _existence_successor_sorter.push(ExistenceSuccessorMsg{request.swap_id, current_edge, last_swap});
                    DEBUG_MSG(_display_debug, "Inform swap " << request.swap_id << " that " << last_swap << " is a successor for edge " << current_edge);
                }

                last_swap = request.swap_id;
                foundTargetEdge = (foundTargetEdge || !request.forward_only);
            }

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
    }

    /*
     * Information sources:
     *  _swaps contains definition of swaps
     *  _depchain_successor_sorter stores swaps we need to inform about our actions
     */
    void EdgeSwapParallelTFP::_perform_swaps() {
        if (_depchain_thread) _depchain_thread->join();

#ifdef EDGE_SWAP_DEBUG_VECTOR
        // debug only
        debug_vector::bufwriter_type debug_vector_writer(_result);
#endif

        // we need to use a desc-comparator since the pq puts the largest element on top
        using ExistenceInfoComparator = typename GenericComparatorStruct<ExistenceInfoMsg>::Descending;
        using ExistenceInfoPQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<ExistenceInfoMsg, ExistenceInfoComparator, _pq_mem, 1 << 20>::result;
        using ExistenceInfoPQBlock = typename ExistenceInfoPQ::block_type;
        stxxl::read_write_pool<ExistenceInfoPQBlock> existence_info_pool(_pq_pool_mem / 2 / ExistenceInfoPQBlock::raw_size,
                                                                          _pq_pool_mem / 2 / ExistenceInfoPQBlock::raw_size);
        ExistenceInfoPQ existence_info_pq(existence_info_pool);
        PQSorterMerger<ExistenceInfoPQ, ExistenceInfoSorter> existence_info_pqsort(existence_info_pq, _existence_info_sorter);

        // use pq in addition to _depchain_edge_sorter to pass messages between swaps
        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;
        using DependencyChainEdgePQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<DependencyChainEdgeMsg, DependencyChainEdgeComparatorPQ, _pq_mem, 1 << 20>::result;
        using DependencyChainEdgePQBlock = typename DependencyChainEdgePQ::block_type;

        stxxl::read_write_pool<DependencyChainEdgePQBlock>
              pq_pool(_pq_pool_mem / 2 / DependencyChainEdgePQBlock::raw_size,
                      _pq_pool_mem / 2 / DependencyChainEdgePQBlock::raw_size);
        DependencyChainEdgePQ edge_state_pq(pq_pool);
        PQSorterMerger<DependencyChainEdgePQ, DependencyChainEdgeSorter> edge_state_pqsort(edge_state_pq, _depchain_edge_sorter);

        swapid_t sid = 0;

        std::vector<edge_t> existence_infos;
        #ifndef NDEBUG
            std::vector<edge_t> missing_infos;
        #endif

        for (typename swap_vector::bufreader_type reader(_swaps_begin, _swaps_end); !reader.empty(); ++reader, ++sid) {
            auto &swap = *reader;

            const edgeid_t *edgeids = swap.edges();
            assert(edgeids[0] < edgeids[1]);

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
                    assert(msg.swap_id == sid);
                    assert(msg.edge_id == edgeids[i]);
                    assert(edge_state_pqsort.source() == SrcSorter || !edge_prev_updated[i]);

                    if (!edge_prev_updated[i] || edge_state_pqsort.source() == SrcPriorityQueue)
                        edges[i] = msg.edge;

                    edge_prev_updated[i] = (edge_prev_updated[i] || (edge_state_pqsort.source() == SrcPriorityQueue));

                    ++edge_state_pqsort;
                } while(UNLIKELY(
                    !edge_state_pqsort.empty() &&
                    (*edge_state_pqsort).edge_id == edgeids[i] &&
                    (*edge_state_pqsort).swap_id == sid
                ));
            }

            // compute swapped edges
            std::tie(new_edges[0], new_edges[1]) = _swap_edges(edges[0], edges[1], swap.direction());

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

            // write out debug message
            if (produce_debug_vector) {
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
            for (; !_depchain_successor_sorter.empty(); ++_depchain_successor_sorter) {
                auto &succ = *_depchain_successor_sorter;

                assert(succ.swap_id >= sid);
                if (succ.swap_id > sid)
                    break;

                assert(succ.edge_id == edgeids[0] || succ.edge_id == edgeids[1]);
                assert(succ.successor > sid);

                int successor = (succ.edge_id == edgeids[0] ? 0 : 1);
                if (perform_swap || edge_prev_updated[successor]) {
                    edge_state_pq.push(DependencyChainEdgeMsg{succ.successor, succ.edge_id, edges[successor + 2*perform_swap]});
                }

                successor_found[successor] = true;
            }

            // send current state of edge iff there are no successors to this edge
            for(unsigned int i=0; i<2; i++) {
                if (!successor_found[i]) {
                    _edge_update_sorter.push(edges[i + 2 * perform_swap]);
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
                        existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge});
                    #else
                        existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge, true});
                        DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << true << " to " << succ.successor);
                    #endif
                } else if (succ.edge == edges[0] || succ.edge == edges[1]) {
                    // source edges never exist (if no swap has been performed, this has been handled above)
                    #ifndef NDEBUG
                        existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge, false});
                        DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << false << " to " << succ.successor);
                    #endif
                } else {
                    #ifdef NDEBUG
                        if (std::binary_search(existence_infos.begin(), existence_infos.end(), succ.edge)) {
                            existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge});
                        }
                    #else
                    bool exists = std::binary_search(existence_infos.begin(), existence_infos.end(), succ.edge);
                    existence_info_pq.push(ExistenceInfoMsg{succ.successor, succ.edge, exists});
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

        assert(existence_info_pq.empty());
        //_existence_info_sorter.finish_clear();

        if (_async_processing) {
            _edge_update_sorter_thread.reset(
                new std::thread([&](){_edge_update_sorter.sort();})
            );
        } else {
            _edge_update_sorter.sort();
        }
    }

    void EdgeSwapParallelTFP::run(uint64_t swaps_per_iteration) {
        bool show_stats = true;

        _swaps_begin = _swaps.begin();
        bool first_iteration = true;

        using UpdateStream = EdgeVectorUpdateStream<edge_vector, BoolStream, decltype(_edge_update_sorter)>;

        const auto initial_edge_size = _edges.size();

        BoolStream last_update_mask, new_update_mask;

        while(_swaps_begin != _swaps.end()) {
            if (swaps_per_iteration) {
                _swaps_end = std::min(_swaps.end(), _swaps_begin + swaps_per_iteration);
            } else {
                _swaps_end = _swaps.end();
            }

            _start_stats(show_stats);

            _gather_edges();
            _report_stats("_gather_edges: ", show_stats);

            EdgeIdVector edge_to_update;

            // in the first iteration, we only need to read edges, while in all further
            // we also have to write out changes from the previous iteration
            if (first_iteration) {
                typename edge_vector::bufreader_type reader(_edges);
                _compute_dependency_chain(reader, new_update_mask);
                first_iteration = false;
            } else {
                if (_edge_update_sorter_thread)
                    _edge_update_sorter_thread->join();
                UpdateStream update_stream(_edges, last_update_mask, _edge_update_sorter);
                _compute_dependency_chain(update_stream, new_update_mask);
                update_stream.finish();
                _edge_update_sorter.clear();
            }

            {
                assert(_edges.size() == initial_edge_size);
                stxxl::STXXL_UNUSED(initial_edge_size);
            }

#ifndef NDEBUG
            {
                typename edge_vector::bufreader_type reader(_edges);
                edge_t last_edge = *reader;
                ++reader;
                assert(!last_edge.is_loop());
                for(;!reader.empty();++reader) {
                    auto & edge = *reader;
                    assert(!edge.is_loop());
                    assert(last_edge < edge);
                    last_edge = edge;
                }
            }
#endif

            std::swap(new_update_mask, last_update_mask);

            _report_stats("_compute_dependency_chain: ", show_stats);
            _compute_conflicts();
            _report_stats("_compute_conflicts: ", show_stats);
            _process_existence_requests();
            _report_stats("_process_existence_requests: ", show_stats);
            _perform_swaps();
            _report_stats("_perform_swaps: ", show_stats);

            _swaps_begin = _swaps_end;

            if (_swaps_begin != _swaps.end())
                _reset();
        }

        if (_edge_update_sorter_thread)
            _edge_update_sorter_thread->join();

        UpdateStream update_stream(_edges, last_update_mask, _edge_update_sorter);
        update_stream.finish();

        if (_result_thread) _result_thread->join();
    }
};
