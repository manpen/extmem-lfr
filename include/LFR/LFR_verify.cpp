#include "LFR.h"
#ifndef LFR_NO_VERIFICATION

#include<DistributionCount.h>
#include<GenericComparator.h>
#include<Utils/FloatDistributionCount.h>
#include<Utils/StableAssert.h>

#endif


namespace LFR {
#ifdef LFR_NO_VERIFICATION
    void LFR::_verify_assignment() {
        std::cout << "[LFR::_verify_assignment] is disabled" << std::endl;
    }

    void LFR::_verify_result() {
        std::cout << "[LFR::_verify_assignment] is disabled" << std::endl;
    }

#else
    void LFR::_verify_assignment() {
        community_t com = 0;
        node_t size = 0;
        degree_t max_deg = 0;
        node_t max_member = 0;

        bool invalid = false;

        using node_deg_t = std::pair<node_t, degree_t>;
        using ndcompare_t = GenericComparator<node_deg_t>::Ascending;
        stxxl::sorter<node_deg_t, ndcompare_t> nds(ndcompare_t{}, SORTER_MEM);

        using reader_t = typename decltype(_community_assignments)::bufreader_type;

        // check that community capacity is not exceeded and that all node
        // have sufficiently many neighbors
        for (reader_t reader(_community_assignments); !reader.empty(); ++reader) {
            const CommunityAssignment &a = *reader;

            nds.push({a.node_id, a.degree});

            if (a.community_id == com) {
                size++;

                if (max_deg < a.degree) {
                    max_deg = a.degree;
                    max_member = a.node_id;
                }


            } else {
                STABLE_EXPECT_EQ(size, _community_size(com));

                if (size < max_deg) {
                    std::cerr << "Node " << max_member << " with degree " << max_deg << " assigned to community " << com << " of size " << size << std::endl;
                }

                STABLE_EXPECT(size >= max_deg);
                com = a.community_id;
                size = 1;
                max_deg = a.degree;
            }
        }

        STABLE_EXPECT_EQ(size, _community_size(com));
        STABLE_EXPECT_GE(size, max_deg);

        // check that intra-degree of every node is met
        {
            nds.sort();
            node_t cur_node = (*nds).first;
            degree_t degree = 0;
            _node_sorter.rewind();
            node_t nid = 0;
            community_t memberships = 0;

            auto check_node = [&] () {
                for (; nid < cur_node; ++nid, ++_node_sorter);
                assert(!_node_sorter.empty());
                STABLE_EXPECT_EQ(nid, cur_node);

                auto &ndm = *_node_sorter;

                if (degree != ndm.totalInternalDegree(_mixing) || memberships != ndm.memberships()) {
                    std::cerr << nid << " requested " << ndm.totalInternalDegree(_mixing) << " intra edges"
                              " and " << ndm.memberships() << " memberships. "
                              " intra-deg " << degree << " over " << memberships << std::endl;
                }
            };

            for (; !nds.empty(); ++nds) {
                const node_deg_t &nd = *nds;

                if (cur_node == nd.first) {
                    degree += nd.second;
                    memberships++;
                } else {
                    check_node();
                    cur_node = nd.first;
                    degree = nd.second;
                    memberships = 1;
                }
            }
            check_node();
            ++_node_sorter;
            STABLE_EXPECT(_node_sorter.empty());
        }
        if (invalid)
            abort();
    }

    void LFR::_verify_result_graph() {
        bool invalid = false;
        const bool use_im_checks = _number_of_nodes < (1ll << 28);

        // check:
        //  - no multiedges
        //  - no self-loops
        //  - node deg. distribution matches request

        _edges.consume();
        stxxl::sorter<node_t, GenericComparator<node_t>::Ascending> nodes(GenericComparator<node_t>::Ascending(), SORTER_MEM);

        edge_t last_edge = edge_t::invalid();
        for(_edges.consume(); !_edges.empty(); ++_edges) {
            const auto & edge = *_edges;

            STABLE_EXPECT_NE(last_edge, edge);
            STABLE_EXPECT(!edge.is_loop());

            nodes.push(edge.first);
            nodes.push(edge.second);
        }

        std::vector<degree_t> node_degrees;

        nodes.sort();
        {
            DistributionCount<decltype(nodes), node_t> dc(nodes);
            _node_sorter.rewind();

            if (use_im_checks) {
                node_degrees.resize(_number_of_nodes, 0);
            }

            node_t nid = 0;

            node_t not_matching = 0;
            edgeid_t unmaterialized = 0;
            edgeid_t overassigned = 0;
            node_t nodes_ceiled = 0;

            edgeid_t total_degree = 0;
            for(; !dc.empty(); ++dc) {
                const auto & cur = *dc;
                total_degree += cur.count;

                if (use_im_checks)
                    node_degrees[cur.value] = cur.count;

                for(; nid < cur.value; ++nid, ++_node_sorter);
                const auto & ndm = *_node_sorter;
                nodes_ceiled += ndm.ceil();


                STABLE_EXPECT_LE(cur.count, ndm.degree());

                if (cur.count > ndm.degree()) {
                    overassigned++;
                }

                if (cur.count < ndm.degree()) {
                    unmaterialized += ndm.degree() - cur.count;
                    not_matching++;
                }
            }

            std::cout << "Found " << not_matching << " nodes with too low degree. "
                         "Miss " << unmaterialized << " (" << (static_cast<double>(unmaterialized) / _number_of_nodes) << " per node) edges in total."
            << std::endl;

            std::cout << "Found " << overassigned << " node with too high degree. " << std::endl;
            std::cout << "Nodes ceiled: " << nodes_ceiled << std::endl;

            STABLE_ASSERT_EQ(total_degree, 2*_edges.size());
        }





        // compute intra-degree (IM)
        if (use_im_checks) {
            // load ndms
            std::vector<NodeDegreeMembership> ndms(_number_of_nodes);
            _node_sorter.rewind();
            stxxl::stream::materialize(_node_sorter, ndms.begin());

            // compute membership prefix sum
            std::vector<edgeid_t> member_offset;
            member_offset.reserve(_number_of_nodes+1);
            edgeid_t no_memberships = 0;
            for(const auto &ndm : ndms) {
                member_offset.push_back(no_memberships);
                no_memberships += ndm.memberships();
            }
            member_offset.push_back(no_memberships);

            std::vector<community_t> memberships(no_memberships);
            std::vector<decltype(memberships)::iterator> node_writers(_number_of_nodes);
            std::transform(member_offset.begin(), member_offset.begin()+_number_of_nodes, node_writers.begin(),
                           [&memberships] (const edgeid_t & off) {return memberships.begin() + off;}
            );

            for(typename decltype(_community_assignments)::bufreader_type reader(_community_assignments);
                !reader.empty(); ++reader) {
                const CommunityAssignment& as = *reader;

                auto & ptr = node_writers.at(as.node_id);
                auto dist = std::distance(memberships.begin(), ptr);
                STABLE_EXPECT_LS(dist, member_offset.at(as.node_id+1));
                *ptr = as.community_id;
                ++ptr;
            }

            for(node_t nid=0; nid < _number_of_nodes; ++nid) {
                auto begin = memberships.begin() + member_offset[nid  ];
                auto end   = memberships.begin() + member_offset[nid+1];

                STABLE_EXPECT(node_writers[nid] == end);

                std::sort(begin, end);
            }

            node_writers.clear();

            auto is_intra_edge = [&] (const edge_t& edge) {
                auto it1 = memberships.begin()+member_offset.at(edge.first);
                auto end1 = memberships.begin()+member_offset.at(edge.first+1)-1;
                auto it2 = memberships.begin()+member_offset.at(edge.second);
                auto end2 = memberships.begin()+member_offset.at(edge.second+1)-1;

                while(it1 != end1 || it2 != end2) {
                    if (*it1 == *it2)
                        return true;

                    if (it1 == end1)
                        ++it2;
                    else if (it2 == end2)
                        ++it1;
                    else if (*it1 < *it2)
                        ++it1;
                    else
                        ++it2;
                }

                return *it1 == *it2;
            };

            std::vector<degree_t> intra_degrees(_number_of_nodes, 0);

            edgeid_t intra_edges = 0;

            for(_edges.consume(); !_edges.empty(); ++_edges) {
                const auto &edge = *_edges;
                bool intra = is_intra_edge(edge);
                if (!intra) continue;

                intra_degrees.at(edge.first)++;
                intra_degrees.at(edge.second)++;
                intra_edges++;
            }

            double mixing = 1.0 - static_cast<double>(intra_edges) / _edges.size();
            std::cout << "Mixing: " << mixing << std::endl;
            STABLE_EXPECT_LS(std::abs(mixing - _mixing) / _mixing, 0.05);

            std::cout << "Resulting graph has " << _edges.size() << " edges, " << intra_edges << " of them are intra-community edges and "
                      << (_edges.size() - intra_edges) <<  " of them are inter-community edges. Mixing: " << mixing
                      << std::endl;

            double stdev = 0.;
             for(node_t nid=0; nid < _number_of_nodes; ++nid) {
                STABLE_EXPECT_LE(intra_degrees[nid], ndms[nid].totalInternalDegree(_mixing));
                STABLE_EXPECT_LE(node_degrees[nid] - intra_degrees[nid], ndms[nid].externalDegree(_mixing));

                double diff =  (1.0 - static_cast<double>(intra_degrees[nid]) / node_degrees[nid]) - mixing;
                stdev += diff * diff;
            }
            std::cout << "Mixing stdev" << std::sqrt(stdev) / (_number_of_nodes - 1) << std::endl;

        } else {
            std::cout << "Skipped in-depth IM analysis" << std::endl;
        }

        if (invalid)
            abort();
    }

#endif
}