#include <EdgeSwapTFP.hpp>

#include <algorithm>
#include <stx/btree_map>

#include <PQSorterMerger.hpp>

#include <stxxl/priority_queue>

#include <vector>

namespace EdgeSwapTFP {
    template<class EdgeVector, class SwapVector, bool compute_stats>
    void EdgeSwapTFP<EdgeVector, SwapVector, compute_stats>::_compute_dependency_chain() {
        using edge_swap_msg_t = std::tuple<edgeid_t, swapid_t>;
        using edge_swap_sorter_t = stxxl::sorter<edge_swap_msg_t, GenericComparatorTuple<edge_swap_msg_t>::Ascending>;
        edge_swap_sorter_t edge_swap_sorter(GenericComparatorTuple<edge_swap_msg_t>::Ascending(), _sorter_mem);

        // Every swap k to edges i, j sends one message (edge-id, swap-id) to each edge.
        // We then sort the messages lexicographically to gather all requests to an edge
        // at the same place.
        {
            swapid_t sid = 0;
            for (typename SwapVector::bufreader_type reader(_swaps_begin, _swaps_end); !reader.empty(); ++reader, ++sid) {
                auto &swap_desc = *reader;
                edge_swap_sorter.push(edge_swap_msg_t(swap_desc.edges()[0], sid));
                edge_swap_sorter.push(edge_swap_msg_t(swap_desc.edges()[1], sid));
            }
            edge_swap_sorter.sort();
        }

        typename EdgeVector::bufreader_type edge_reader(_edges);
        edgeid_t eid = 0; // points to the next edge that can be read

        swapid_t last_swap = 0; // initialize to make gcc shut up

        stx::btree_map<uint_t, uint_t> swaps_per_edges;
        uint_t swaps_per_edge = 1;

        // For every edge we send the incident vertices to the first swap,
        // i.e. the request with the lowest swap-id. We get this info by scanning
        // through the original edge list and the sorted request list in parallel
        // (i.e. by "merging" them). If there are multiple requests to an edge, we
        // send each predecessor the id of the next swap possibly affecting this edge.
        for (; !edge_swap_sorter.empty(); ++edge_swap_sorter) {
            edgeid_t requested_edge;
            swapid_t requesting_swap;
            std::tie(requested_edge, requesting_swap) = *edge_swap_sorter;

            // move reader buffer until we found the edge
            for (; eid < requested_edge; ++eid, ++edge_reader) {
                assert(!edge_reader.empty());
            }

            // read edge and sent it to next node, if
            if (eid == requested_edge) {
                assert(!edge_reader.empty());
                const edge_t &edge = *edge_reader;

                _depchain_edge_sorter.push({requesting_swap, requested_edge, edge});

                ++edge_reader;
                ++eid;

                if (compute_stats) {
                    swaps_per_edges[swaps_per_edge]++;
                    swaps_per_edge = 1;
                }
            } else {
                _depchain_successor_sorter.push(DependencyChainSuccessorMsg{last_swap, requested_edge, requesting_swap});
                DEBUG_MSG(_display_debug, "Report to swap " << last_swap << " that swap " << requesting_swap << " needs edge " << requested_edge);
                if (compute_stats)
                    swaps_per_edge++;
            }

            last_swap = requesting_swap;
        }

        if (compute_stats) {
            swaps_per_edges[swaps_per_edge]++;

            for (const auto &it : swaps_per_edges) {
                std::cout << it.first << " " << it.second << " #SWAPS-PER-EDGE" << std::endl;
            }
        }

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
     * These configurations are kept in depchain_edge_pq: Each swap receives
     * the complete state set of both edges and computes the cartesian product
     * of both. If there exists a successor swap (info stored in
     * _depchain_successor_sorter), i.e. a swap that will be  affect by the
     * current one, these information are forwarded.
     *
     * We further request information whether the edge exists by pushing requests
     * into _existence_request_sorter.
     */
    template<class EdgeVector, class SwapVector, bool compute_stats>
    void EdgeSwapTFP<EdgeVector, SwapVector, compute_stats>::_compute_conflicts() {
        swapid_t sid = 0;
        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;
        using DependencyChainEdgePQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<DependencyChainEdgeMsg, DependencyChainEdgeComparatorPQ, _pq_mem, 1 << 20>::result;
        using DependencyChainEdgePQBlock = typename DependencyChainEdgePQ::block_type;

        // use pq in addition to _depchain_edge_sorter to pass messages between swaps
        stxxl::read_write_pool<DependencyChainEdgePQBlock>
              pq_pool(_pq_pool_mem / 2 / DependencyChainEdgePQBlock::raw_size,
                      _pq_pool_mem / 2 / DependencyChainEdgePQBlock::raw_size);
        DependencyChainEdgePQ depchain_edge_pq(pq_pool);
        PQSorterMerger<DependencyChainEdgePQ, DependencyChainEdgeSorter>
              depchain_pqsort(depchain_edge_pq, _depchain_edge_sorter);

        // statistics
        uint_t duplicates_dropped = 0;
        stx::btree_map<uint_t, uint_t> state_sizes;

        std::array<std::vector<edge_t>, 2> insertion_sets;

        std::vector<edge_t> edges[2];
        for (typename SwapVector::bufreader_type reader(_swaps_begin, _swaps_end); !reader.empty(); ++reader, ++sid) {
            auto &swap = *reader;

            swapid_t successors[2];

            // we directly insert into the PQ, so we might have to update the merger
            depchain_pqsort.update();

            // fetch messages sent to this edge
            for (unsigned int i = 0; i < 2; i++) {
                edges[i].clear();
                const auto eid = swap.edges()[i];

                // get successor
                if (!_depchain_successor_sorter.empty()) {
                    auto &msg = *_depchain_successor_sorter;

                    assert(msg.swap_id >= sid);
                    assert(msg.swap_id > sid || msg.edge_id >= eid);

                    if (msg.swap_id != sid || msg.edge_id != eid) {
                        successors[i] = 0;
                    } else {
                        DEBUG_MSG(_display_debug, "Got successor for S" << sid << ", E" << eid << ": " << msg);
                        successors[i] = msg.successor;
                        ++_depchain_successor_sorter;
                    }

                } else {
                    successors[i] = 0;
                }


                // fetch possible edge state before swap
                while (!depchain_pqsort.empty()) {
                    auto msg = *depchain_pqsort;

                    if (msg.swap_id != sid || msg.edge_id != eid)
                        break;

                    ++depchain_pqsort;

                    if (_deduplicate_before_insert || edges[i].empty() || (msg.edge != edges[i].back())) {
                        edges[i].push_back(msg.edge);
                    } else if (compute_stats && !edges[i].empty()) {
                        duplicates_dropped++;
                    }
                }

                DEBUG_MSG(_display_debug, "SWAP " << sid << " Edge " << eid << " Successor: " << successors[i] << " States: " << edges[i].size());

                // ensure that we received at least one state of the edge before the swap
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

            if (compute_stats) {
                state_sizes[edges[0].size() +edges[1].size()]++;
            }

            // compute "cartesian" product between possible edges to determine all possible new edges
            // TODO: Check for duplicates
            for (auto &e1 : edges[0]) {
                for (auto &e2 : edges[1]) {
                    edge_t new_edges[2];
                    std::tie(new_edges[0], new_edges[1]) = _swap_edges(e1, e2, swap.direction());

                    for (unsigned int i = 0; i < 2; i++) {
                        auto &new_edge = new_edges[i];

                        if (_deduplicate_before_insert) {
                            insertion_sets[i].push_back(new_edge);
                        } else {
                            // send new edge to successor swap
                            if (successors[i]) {
                                depchain_edge_pq.push(DependencyChainEdgeMsg{successors[i], swap.edges()[i], new_edge});
                            }

                            // register to receive information on whether this edge exists
                            _existence_request_sorter.push(ExistenceRequestMsg{new_edge, sid});

                            DEBUG_MSG(_display_debug, "Swap " << sid << " may yield " << new_edge << " at " << swap.edges()[i]);
                        }
                    }
                }
            }

            for (unsigned int i = 0; i < 2; i++) {
                for (auto &edge : edges[i]) {
                    if (_deduplicate_before_insert) {
                        insertion_sets[i].push_back(edge);
                    } else {
                        if (successors[i]) {
                            depchain_edge_pq.push(DependencyChainEdgeMsg{successors[i], swap.edges()[i], edge});
                        }
                        _existence_request_sorter.push(ExistenceRequestMsg{edge, sid});
                    }
                }
            }

            if (_deduplicate_before_insert) {
                for(unsigned int i=0; i < 2; i++) {
                    auto & is = insertion_sets[i];

                    if (is.size() > 2)
                        std::sort(is.begin(), is.end());

                    edge_t last_edge(-1, -1);
                    for(auto & edge : is) {
                        if (edge == last_edge)
                            continue;

                        if (successors[i]) {
                            depchain_edge_pq.push(DependencyChainEdgeMsg{successors[i], swap.edges()[i], edge});
                        }

                        _existence_request_sorter.push(ExistenceRequestMsg{edge, sid});

                        last_edge = edge;
                    }

                    is.clear();
                }
            }
        }

        if (compute_stats) {
            std::cout << "Dropped " << duplicates_dropped << " duplicates in edge-state information in _compute_conflicts()" << std::endl;
            for (const auto &it : state_sizes) {
                std::cout << it.first << " " << it.second << " #STATE-SIZE" <<
                std::endl;
            }
        }

        _existence_request_sorter.sort();
        _depchain_successor_sorter.rewind();
        _depchain_edge_sorter.rewind();
    }

    /*
     * We parallel stream through _edges and _existence_request_sorter
     * to check whether a requested edge exists in the input graph.
     * The result is sent to the first swap requesting using
     * _existence_info_pq. We additionally compute a dependency chain
     * by informing every swap about the next one requesting the info.
     */
    template<class EdgeVector, class SwapVector, bool compute_stats>
    void EdgeSwapTFP<EdgeVector, SwapVector, compute_stats>::_process_existence_requests() {
        typename EdgeVector::bufreader_type edge_reader(_edges);

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

            // TODO: we can omit the first existence info if the edge does not exists
            //_existence_info_sorter.push(ExistenceInfoMsg{request.swap_id, 0, current_edge, exists});
            if (exists)
                _existence_info_sorter.push({request.swap_id, current_edge});


            swapid_t last_swap = request.swap_id;
            for (++_existence_request_sorter; !_existence_request_sorter.empty(); ++_existence_request_sorter) {
                auto &request = *_existence_request_sorter;
                if (request.edge != current_edge)
                    break;

                if (last_swap == request.swap_id)
                    continue;

                // inform an earlier swap about later swaps that need the new state
                assert(last_swap < request.swap_id);
                _existence_successor_sorter.push(ExistenceSuccessorMsg{last_swap, current_edge, request.swap_id});
                last_swap = request.swap_id;
            }
        }

        _existence_request_sorter.finish_clear();
        _existence_successor_sorter.sort();
        _existence_info_sorter.sort();
    }

    /*
     * Information sources:
     *  _swaps contains definition of swaps
     *  _depchain_successor_sorter stores swaps we need to inform about our actions
     */
    template<class EdgeVector, class SwapVector, bool compute_stats>
    void EdgeSwapTFP<EdgeVector, SwapVector, compute_stats>::_perform_swaps() {
        // debug only
        debug_vector::bufwriter_type debug_vector_writer(_result);

        // we need to use a desc-comparator since the pq puts the largest element on top
        using ExistenceInfoComparator = typename GenericComparatorStruct<ExistenceInfoMsg>::Descending;
        using ExistenceInfoPQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<ExistenceInfoMsg, ExistenceInfoComparator, _pq_mem, 1 << 20>::result;
        using ExistenceInfoPQBlock = typename ExistenceInfoPQ::block_type;
        stxxl::read_write_pool<ExistenceInfoPQBlock> existence_info_pool(_pq_pool_mem / 2 / ExistenceInfoPQBlock::raw_size,
                                                                          _pq_pool_mem / 2 / ExistenceInfoPQBlock::raw_size);
        ExistenceInfoPQ existence_info_pq(existence_info_pool);

        // use pq in addition to _depchain_edge_sorter to pass messages between swaps
        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;
        using DependencyChainEdgePQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<DependencyChainEdgeMsg, DependencyChainEdgeComparatorPQ, _pq_mem, 1 << 20>::result;
        using DependencyChainEdgePQBlock = typename DependencyChainEdgePQ::block_type;

        stxxl::read_write_pool<DependencyChainEdgePQBlock>
              pq_pool(_pq_pool_mem / 2 / DependencyChainEdgePQBlock::raw_size,
                      _pq_pool_mem / 2 / DependencyChainEdgePQBlock::raw_size);
        DependencyChainEdgePQ edge_state_pq(pq_pool);
        PQSorterMerger<DependencyChainEdgePQ, DependencyChainEdgeSorter>
              edge_state_pqsort(edge_state_pq, _depchain_edge_sorter);

        swapid_t sid = 0;

        using existence_info_t = std::pair<edge_t, bool>;
        std::vector<existence_info_t> existence_infos;
        existence_infos.reserve(1024);
        auto find_existence_info = [&existence_infos] (const edge_t & edge) {
           return std::find_if(existence_infos.begin(), existence_infos.end(),
                [&edge] (const existence_info_t & it) {return it.first == edge;});
//               std::lower_bound(
//                 existence_infos.begin(), existence_infos.end(), edge,
//                 [] (const existence_info_t& pair, const edge_t& v) {return v >= pair.first;}
//           );
        };


        for (typename SwapVector::bufreader_type reader(_swaps_begin, _swaps_end); !reader.empty(); ++reader, ++sid) {
            auto &swap = *reader;

            const edgeid_t *edgeids = swap.edges();
            assert(edgeids[0] < edgeids[1]);

            edge_state_pqsort.update();

            // collect the current state of the edge to be swapped
            edge_t edges[4];
            edge_t *new_edges = edges + 2;
            for (unsigned int i = 0; i < 2; i++) {
                assert(!edge_state_pqsort.empty());
                auto & msg = *edge_state_pqsort;
                assert(msg.swap_id == sid);
                assert(msg.edge_id == edgeids[i]);

                edges[i] = msg.edge;
                ++edge_state_pqsort;
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

            // gather all edge states that have been sent to this swap;
            // due to the PQ the edges are sorted and we can perform a bin-search later on
            //std::map<edge_t, bool> existence_infos;
            existence_infos.clear();
            while(1) {
                const bool sorter_active = !_existence_info_sorter.empty() && (*_existence_info_sorter).swap_id == sid;
                const bool pq_active = !existence_info_pq.empty() && existence_info_pq.top().swap_id == sid;
                if (!sorter_active && !pq_active) break;

                if (!pq_active || (sorter_active && ((*_existence_info_sorter).edge <= existence_info_pq.top().edge))) {
                    existence_infos.push_back(std::make_pair((*_existence_info_sorter).edge, true));
                    ++_existence_info_sorter;

                } else {
                    auto &msg = existence_info_pq.top();

                    if (existence_infos.empty() || existence_infos.back().first != msg.edge) {
                        existence_infos.push_back(std::make_pair(msg.edge, msg.exists));
                    } else {
                        existence_infos.back().second = msg.exists;
                    }

                    existence_info_pq.pop();
                }
            }

#ifndef NDEBUG
            if (_display_debug) {
                for (auto &k : existence_infos)
                    std::cout << sid << " " << k.first << " " << k.second << std::endl;
            }
#endif

            // check if there's an conflicting edge
            bool conflict_exists[2];
            for (unsigned int i = 0; i < 2; i++) {
                const auto state = find_existence_info(new_edges[i]);
                conflict_exists[i] = (state != existence_infos.end()) ? state->second : false;
            }

            // can we perform the swap?
            const bool loop = new_edges[0].is_loop() || new_edges[1].is_loop();
            const bool perform_swap = !(conflict_exists[0] || conflict_exists[1] || loop);

            // write out debug message
            {
                SwapResult res;
                res.performed = perform_swap;
                res.loop = loop;
                std::copy(new_edges, new_edges + 2, res.edges);
                for(unsigned int i=0; i < 2; i++) {
                    res.edges[i] = new_edges[i];
                    res.conflictDetected[i] = conflict_exists[i];
                }
                res.normalize();

                debug_vector_writer << res;
                DEBUG_MSG(_display_debug, "Swap " << sid << " " << res);
            }

            for (unsigned int i = 0; i < 2; i++) {
                // update temporary structure
                find_existence_info(edges[i])->second = !perform_swap;
                if (perform_swap) {
                    const auto & state = find_existence_info(new_edges[i]);
                    if (state == existence_infos.end()) {
                        existence_infos.push_back({new_edges[i], true});
                    } else {
                        state->second = true;
                    }
                }

                // issue update of edge list
                if (perform_swap)
                    _edge_update_sorter.push(EdgeUpdateMsg{edgeids[i], sid, new_edges[i]});
            }

            // forward edge state to successor swap
            for (; !_depchain_successor_sorter.empty(); ++_depchain_successor_sorter) {
                auto &succ = *_depchain_successor_sorter;

                assert(succ.swap_id >= sid);
                if (succ.swap_id > sid)
                    break;

                assert(succ.edge_id == edgeids[0] || succ.edge_id == edgeids[1]);
                assert(succ.successor > sid);

                edge_state_pq.push(DependencyChainEdgeMsg{
                      succ.successor,
                      succ.edge_id,
                      edges[(succ.edge_id == edgeids[0] ? 0 : 1) + (perform_swap ? 2 : 0)]
                });
            }

            // forward existence information
            for (; !_existence_successor_sorter.empty(); ++_existence_successor_sorter) {
                auto &succ = *_existence_successor_sorter;

                assert(succ.swap_id >= sid);
                if (succ.swap_id > sid) break;

                const auto state = find_existence_info(succ.edge);
                const bool exists = (state != existence_infos.end()) ? state->second : false;

                existence_info_pq.push(ExistenceInfoMsg{succ.successor, sid, succ.edge, exists});
                DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << exists << " to " << succ.successor);
            }
        }

        debug_vector_writer.finish();

        // check message data structures are empty
        assert(_depchain_successor_sorter.empty());
        _depchain_successor_sorter.finish_clear();

        assert(_existence_successor_sorter.empty());
        _existence_successor_sorter.finish_clear();

        assert(existence_info_pq.empty());
        _existence_info_sorter.finish_clear();

        _edge_update_sorter.sort();
    }

    /*
     * During the processing of swaps, we produce an vector of updates to
     * the edges. If multiple updates are scheduled for the same only the
     * latest write request is applied. After the writes are performed,
     * the edge list is sorted and materialized into _edges.
     */
    template<class EdgeVector, class SwapVector, bool compute_stats>
    void EdgeSwapTFP<EdgeVector, SwapVector, compute_stats>::_apply_updates() {
        // the sorter will collect all new graph edges (either copied from
        // the original graph or updated through the _edge_update_sorter)
        stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> edge_sorter(GenericComparator<edge_t>::Ascending(), _sorter_mem);

        typename EdgeVector::bufreader_type edge_reader(_edges);
        for (edgeid_t eid = 0; !edge_reader.empty(); ++edge_reader, ++eid) {
            edge_t edge = *edge_reader;

            for (; !_edge_update_sorter.empty(); ++_edge_update_sorter) {
                auto &update = *_edge_update_sorter;
                assert(update.edge_id >= eid);
                if (update.edge_id > eid)
                    break;

                DEBUG_MSG(_display_debug, "Got update " << update.to_tuple());
                edge = update.updated_edge;
            }

            if (edge.first > edge.second)
                std::swap(edge.first, edge.second);

            edge_sorter.push(edge);
        }

        edge_sorter.sort();
        stxxl::stream::materialize(edge_sorter, _edges.begin());
    }

    template<class EdgeVector, class SwapVector, bool compute_stats>
    void EdgeSwapTFP<EdgeVector, SwapVector, compute_stats>::run(uint64_t swaps_per_iteration) {
        bool show_stats = true;

        _swaps_begin = _swaps.begin();
        while(_swaps_begin != _swaps.end()) {
            if (swaps_per_iteration) {
                _swaps_end = std::min(_swaps.end(), _swaps_begin + swaps_per_iteration);
            } else {
                _swaps_end = _swaps.end();
            }

            _start_stats(show_stats);
            _compute_dependency_chain();
            _report_stats("_compute_dependency_chain: ", show_stats);
            _compute_conflicts();
            _report_stats("_compute_conflicts: ", show_stats);
            _process_existence_requests();
            _report_stats("_process_existence_requests: ", show_stats);
            _perform_swaps();
            _report_stats("_perform_swaps: ", show_stats);
            _apply_updates();
            _report_stats("_apply_updates: ", show_stats);

            _swaps_begin = _swaps_end;

            if (_swaps_begin != _swaps.end())
                _reset();
        }
    }

    template class EdgeSwapTFP<stxxl::vector<edge_t>, stxxl::vector<SwapDescriptor>, false>;
    template class EdgeSwapTFP<stxxl::vector<edge_t>, stxxl::vector<SwapDescriptor>, true>;
};
