#include <EdgeSwaps/EdgeSwapInternalSwapsBase.h>


void EdgeSwapInternalSwapsBase::simulateSwapsAndGenerateEdgeExistenceQuery(const std::vector< EdgeSwapBase::swap_descriptor > &swaps, const std::vector< edge_t > &edges, const std::array<std::vector< bool >, 2>& swap_has_successor) {
    // stores for a swap and the position in the swap (0,1) the edge
    struct swap_edge_t {
        internal_swapid_t sid;
        unsigned char spos;
        edge_t e;

        DECL_LEX_COMPARE(swap_edge_t, sid, spos, e);
    };


    { // find possible conflicts
        // construct possible conflict pairs
        std::vector<std::vector<edge_t>> possibleEdges(edges.size());
        std::vector<edge_t> current_edges[2];
        std::vector<edge_t> new_edges[2];

        for (auto s_it = swaps.begin(); s_it != swaps.end(); ++s_it) {
            const internal_swapid_t sid = (s_it - swaps.begin());
            const auto& eids = s_it->edges();

            if (eids[0] == eids[1] || eids[0] == -1) continue;

            for (unsigned char spos = 0; spos < 2; ++spos) {
                current_edges[spos].clear();
                new_edges[spos].clear();
                if (! possibleEdges[eids[spos]].empty()) {
                    // remove the vector from possibleEdges and thus free it
                    current_edges[spos] = std::move(possibleEdges[eids[spos]]);
                }

                current_edges[spos].push_back(edges[eids[spos]]);

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
                            new_edges[spos].push_back(t[spos]);
                        }
                    }
                }
            }

            for (unsigned char spos = 0; spos < 2; ++spos) {
                if (new_edges[spos].size() > 1) { // remove duplicates
                    std::sort(new_edges[spos].begin(), new_edges[spos].end());
                    auto last = std::unique(new_edges[spos].begin(), new_edges[spos].end());
                    new_edges[spos].erase(last, new_edges[spos].end());
                }

                for (const auto &e : new_edges[spos]) {
                    _query_sorter.push(edge_existence_request_t {e, sid, false});
                }

                if (swap_has_successor[spos][sid]) {
                    // reserve enough memory to accomodate all elements
                    possibleEdges[eids[spos]].clear();
                    possibleEdges[eids[spos]].reserve(current_edges[spos].size() + new_edges[spos].size());

                    current_edges[spos].pop_back(); // remove the added original edge as it will be loaded again anyway!
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


void EdgeSwapInternalSwapsBase::performSwaps(const std::vector<swap_descriptor>& swaps, std::vector<edge_t> &edges) {
    auto edge_existence_succ_it = _edge_existence_successors.begin();
    std::vector<std::pair<edge_t, community_t>> current_existence;

    auto getNumExistences = [&](edge_t e, bool mustExist = false) -> community_t {
        auto it = std::partition_point(current_existence.begin(), current_existence.end(), [&](const std::pair<edge_t, community_t>& a) { return a.first < e; });
        assert(it != current_existence.end() && it->first == e); // only valid if assertions are on...

        if (it != current_existence.end() && it->first == e) {
            return it->second;
        } else {
            if (UNLIKELY(mustExist)) throw std::logic_error("Error, edge existence information missing that should not be missing");
            return 0;
        }
    };

    swapid_t no_performed = 0;
    swapid_t no_conflicts = 0;
    swapid_t no_loops = 0;
    for (auto s_it = swaps.begin(); s_it != swaps.end(); ++s_it) {
        const auto& eids = s_it->edges();

        if (eids[0] == eids[1] || eids[0] == -1) continue;

        const internal_swapid_t sid = (s_it - swaps.begin());
        edge_t s_edges[2] = {edges[eids[0]], edges[eids[1]]};
        edge_t t_edges[2];
        current_existence.clear();

        SwapResult result;

        //std::cout << "Testing swap of " << e0.first << ", " << e0.second << " and " << e1.first << ", " << e1.second << std::endl;

        std::tie(t_edges[0], t_edges[1]) = _swap_edges(s_edges[0], s_edges[1], s_it->direction());

        assert(_edge_existence_pq.empty() || _edge_existence_pq.front().sid >= sid);

        while (!_edge_existence_pq.empty() && _edge_existence_pq.front().sid == sid) {
            current_existence.emplace_back(_edge_existence_pq.front().e, _edge_existence_pq.front().numExistences);

            std::pop_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
            _edge_existence_pq.pop_back();
        }

        //std::cout << "Target edges " << t0.first << ", " << t0.second << " and " << t1.first << ", " << t1.second << std::endl;


        {
            // compute whether swap can be performed and write debug info out
            result.edges[0] = t_edges[0];
            result.edges[1] = t_edges[1];
            result.loop = (t_edges[0].first == t_edges[0].second || t_edges[1].first == t_edges[1].second);

            result.conflictDetected[0] = result.loop ? false : (getNumExistences(t_edges[0]) > 0);
            result.conflictDetected[1] = result.loop ? false : (getNumExistences(t_edges[1]) > 0);

            result.performed = !result.loop && !(result.conflictDetected[0] || result.conflictDetected[1]);

            no_performed += result.performed;
            no_conflicts += result.conflictDetected[0] + result.conflictDetected[1];

            no_loops += result.loop;

#ifdef EDGE_SWAP_DEBUG_VECTOR
            result.normalize();
            _debug_vector_writer << result;
#endif
        }

        if (0)
        std::cout << s_edges[0] << "\t" << s_edges[1] << "\t -> \t"
                  << t_edges[0] << (getNumExistences(t_edges[0]) > 0)  << "\t"
                  << t_edges[1] << (getNumExistences(t_edges[1]) > 0)  << "\t"
        <<std::endl;



        //std::cout << result << std::endl;

        //assert(result.loop || ((existsEdge.find(t_edges[0]) != existsEdge.end() && existsEdge.find(t_edges[1]) != existsEdge.end())));

        if (result.performed) {
            edges[eids[0]] = t_edges[0];
            edges[eids[1]] = t_edges[1];
        }



        while (edge_existence_succ_it != _edge_existence_successors.end() && edge_existence_succ_it->from_sid == sid) {
            if (result.performed && (edge_existence_succ_it->e == s_edges[0] || edge_existence_succ_it->e == s_edges[1])) {
                // if the swap was performed, a source edge occurs once less - but it might still exist if it was a multi-edge
                auto t = getNumExistences(edge_existence_succ_it->e, true) - 1;
#ifdef NDEBUG
                if (t > 0) { // without debugging, push only if edge still exists
#endif
                    _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e, t});
                    std::push_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
#ifdef NDEBUG
                }
#endif
            } else if (result.performed && (edge_existence_succ_it->e == t_edges[0] || edge_existence_succ_it->e == t_edges[1])) {
                // target edges always exist once if the swap was performed as we create no new multi-edges
                _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e, 1});
                std::push_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
            } else {
                    auto t = getNumExistences(edge_existence_succ_it->e);
#ifdef NDEBUG
                    if (t > 0) { // without debugging, push only if edge still exists
#endif
                        _edge_existence_pq.push_back(edge_existence_answer_t {edge_existence_succ_it->to_sid, edge_existence_succ_it->e, t});
                        std::push_heap(_edge_existence_pq.begin(), _edge_existence_pq.end(), std::greater<edge_existence_answer_t>());
#ifdef NDEBUG
                    }
#endif
            }

            ++edge_existence_succ_it;
        }
    }

    std::cout << "Performed " << no_performed << " out of " << swaps.size() << " swaps ("
              "loops: " << no_loops << " conflicts: " << no_conflicts << ")"
              << std::endl;
};

