#include <EdgeSwapTFP.hpp>

#include <algorithm>
#include <stx/btree_map>


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
            for (typename SwapVector::bufreader_type reader(_swaps); !reader.empty(); ++reader, ++sid) {
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

                _depchain_edge_pq.push({requesting_swap, requested_edge, edge});
                // TODO: may be cheaper to copy _depchain_edge_pq at the end of this function; but this is currently not possible
                _edge_state_pq.push({requesting_swap, requested_edge, edge});

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
    }

    /*
     * Since we do not yet know whether an swap can be performed, we keep for
     * every edge id a set of possible states. Initially this state is only the
     * edge as fetched in _compute_dependency_chain(), but after the first swap
     * the set contains at least two configurations, i.e. the original state
     * (in case the swap cannot be performed) and the swapped state.
     *
     * These configurations are kept in _depchain_edge_pq: Each swap receives
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

        uint_t duplicates_dropped = 0;
        stx::btree_map<uint_t, uint_t> state_sizes;

        for (typename SwapVector::bufreader_type reader(_swaps); !reader.empty(); ++reader, ++sid) {
            auto &swap = *reader;

            swapid_t successors[2];
            std::list<edge_t> edges[2];

            // fetch messages sent to this edge
            for (unsigned int i = 0; i < 2; i++) {
                const auto eid = swap.edges()[i];

                // get successor
                if (!_depchain_successor_sorter.empty()) {
                    auto &msg = *_depchain_successor_sorter;

                    assert(msg.swap_id >= sid);
                    assert(msg.swap_id > sid || msg.edge_id >= eid);

                    if (msg.swap_id != sid || msg.edge_id != eid) {
                        successors[i] = 0;
                    } else {
                        DEBUG_MSG(_display_debug, "Got successor for S" << sid << ", E" << eid << ": " << msg.to_tuple());
                        successors[i] = msg.successor;
                        ++_depchain_successor_sorter;
                    }
                } else {
                    successors[i] = 0;
                }


                // fetch possible edge state before swap
                for (; !_depchain_edge_pq.empty(); _depchain_edge_pq.pop()) {
                    auto &msg = _depchain_edge_pq.top();
                    if (msg.swap_id != sid || msg.edge_id != eid)
                        break;

                    if (edges[i].empty() || msg.edge != edges[i].back()) {
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
            assert(_depchain_edge_pq.empty() || _depchain_edge_pq.top().swap_id > sid);


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

                        // send new edge to successor swap
                        if (successors[i]) {
                            _depchain_edge_pq.push(DependencyChainEdgeMsg{successors[i], swap.edges()[i], new_edge});
                        }

                        // register to receive information on whether this edge exists
                        _existence_request_sorter.push(ExistenceRequestMsg{new_edge, sid});

                        DEBUG_MSG(_display_debug, "Swap " << sid << " may yield " << new_edge << " at " << swap.edges()[i]);
                    }
                }
            }

            for (unsigned int i = 0; i < 2; i++) {
                for (auto &edge : edges[i]) {
                    if (successors[i]) {
                        _depchain_edge_pq.push(DependencyChainEdgeMsg{successors[i], swap.edges()[i], edge});
                    }
                    _existence_request_sorter.push(ExistenceRequestMsg{edge, sid});
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

        _depchain_successor_sorter.rewind();
        _existence_request_sorter.sort();
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
                if (edge > current_edge)
                    break;
                exists = (edge == current_edge);
            }

            // TODO: we can omit the first existence info if the edge does not exists
            _existence_info_pq.push(ExistenceInfoMsg{request.swap_id, 0, current_edge, exists});

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

        swapid_t sid = 0;
        for (typename SwapVector::bufreader_type reader(_swaps); !reader.empty(); ++reader, ++sid) {
            auto &swap = *reader;

            const edgeid_t *edgeids = swap.edges();
            assert(edgeids[0] < edgeids[1]);

            // collect the current state of the edge to be swapped
            edge_t edges[4];
            edge_t *new_edges = edges + 2;
            for (unsigned int i = 0; i < 2; i++) {
                assert(!_edge_state_pq.empty());
                assert(_edge_state_pq.top().swap_id == sid);
                assert(_edge_state_pq.top().edge_id == edgeids[i]);

                edges[i] = _edge_state_pq.top().edge;
                _edge_state_pq.pop();
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
            std::map<edge_t, bool> existence_infos;
            {
                auto it = existence_infos.end();
                for (; !_existence_info_pq.empty() && _existence_info_pq.top().swap_id == sid; _existence_info_pq.pop()) {
                    auto &msg = _existence_info_pq.top();

                    if (it == existence_infos.end() || it->first != msg.edge) {
                        existence_infos.emplace_hint(it, msg.edge, msg.exists);
                    } else {
                        it->second = msg.exists;
                    }
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
                auto it = existence_infos.find(new_edges[i]);

                assert(it != existence_infos.end());
                conflict_exists[i] = (it != existence_infos.end() && it->second);
            }

            // can we perform the swap?
            const bool loop = (new_edges[0].first == new_edges[0].second)
                              || (new_edges[1].first == new_edges[1].second);
            const bool perform_swap = !(conflict_exists[0] || conflict_exists[1] || loop);

            // write out debug message
            {
                SwapResult res;
                res.performed = perform_swap;
                res.loop = loop;
                std::copy(new_edges, new_edges + 2, res.edges);
                std::copy(conflict_exists, conflict_exists + 2, res.conflictDetected);
                res.normalize();

                debug_vector_writer << res;
                DEBUG_MSG(_display_debug, "Swap " << sid << " " << res);
            }

            for (unsigned int i = 0; i < 2; i++) {
                // update temporary structure
                existence_infos[edges[i]] = !perform_swap;
                if (perform_swap) {
                    existence_infos[new_edges[i]] = true;
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

                _edge_state_pq.push(DependencyChainEdgeMsg{
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

                const auto &state = existence_infos.find(succ.edge);
                // TODO: Remains true as long as we sent messages for none existing edges
                assert(state != existence_infos.end());

                _existence_info_pq.push(ExistenceInfoMsg{succ.successor, sid, succ.edge, state->second});
                DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << state->second << " to " << succ.successor);
            }
        }

        debug_vector_writer.finish();

        // check message data structures are empty
        assert(_depchain_successor_sorter.empty());
        _depchain_successor_sorter.finish_clear();

        assert(_existence_successor_sorter.empty());
        _existence_successor_sorter.finish_clear();

        assert(_existence_info_pq.empty());

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
    void EdgeSwapTFP<EdgeVector, SwapVector, compute_stats>::run() {
        _start_stats(compute_stats);
        _compute_dependency_chain();
        _report_stats("_compute_dependency_chain: ", compute_stats);
        _compute_conflicts();
        _report_stats("_compute_conflicts: ", compute_stats);
        _process_existence_requests();
        _report_stats("_process_existence_requests: ", compute_stats);
        _perform_swaps();
        _report_stats("_perform_swaps: ", compute_stats);
        _apply_updates();
        _report_stats("_apply_updates: ", compute_stats);
    }

    template class EdgeSwapTFP<stxxl::vector<edge_t>, stxxl::vector<SwapDescriptor>, false>;
    template class EdgeSwapTFP<stxxl::vector<edge_t>, stxxl::vector<SwapDescriptor>, true>;
};
