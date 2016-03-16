#include "LFR.h"

#include <random>

namespace LFR {

    void LFR::_compute_node_distributions() {
        // setup distributions
        NodeDegreeDistribution ndd(_degree_distribution_params);
        std::default_random_engine generator;
        std::geometric_distribution<int> geo_dist(0.1);

        uint_t degree_sum = 0;
        uint_t memebership_sum = 0;

        if (_overlap_method == geometric) {
            for (uint_t i = 0; i < static_cast<uint_t>(_number_of_nodes); ++i, ++ndd) {
                assert(!ndd.empty());
                auto &degree = *ndd;

                // compute membership
                community_t memberships;
                {
                    degree_t internal_degree = static_cast<degree_t>((1.0 - _mixing) * degree);
                    do {
                        auto r = (1 + geo_dist(generator));
                        memberships = internal_degree / r;
                    } while (
                            !memberships ||
                            memberships > 8 * _community_distribution_params.numberOfNodes / 10 ||
                            internal_degree / memberships > _overlap_config.geometric.maxDegreeIntraDegree
                            );
                }

                _node_sorter.push(NodeDegreeMembership(degree, memberships));
                degree_sum += degree;
                memebership_sum += memberships;
            }
        } else if (_overlap_method == constDegree) {
            for (uint_t i = 0; i < static_cast<uint_t>(_number_of_nodes); ++i, ++ndd) {
                assert(!ndd.empty());
                auto &degree = *ndd;

                community_t memberships = (i < _overlap_config.constDegree.overlappingNodes)
                                          ? _overlap_config.constDegree.multiCommunityDegree : 1;

                const NodeDegreeMembership ndm(degree, memberships);
                assert(ndm.intraCommunityDegree(_mixing, memberships-1));

                //std::cout << "Node " << i <<  " Degree: " << degree << " Mem: " << memberships << std::endl;

                _node_sorter.push(ndm);
            }
        }

        _node_sorter.sort();
        std::cout << "Degree sum: " << degree_sum << " Membership sum: " << memebership_sum << "\n";
    }


    void LFR::_compute_community_size() {
        // allocate memory
        _community_cumulative_sizes.resize(_number_of_communities+1);

        // generate prefix sum of random powerlaw degree distribution
        CommunityDistribution cdd(_community_distribution_params);
        uint_t members_sum = 0;
        auto cum_sum = _community_cumulative_sizes.begin();
        for(; !cdd.empty(); ++cdd, ++cum_sum) {
            *cum_sum = members_sum;
            members_sum += *cdd;
        }
        *cum_sum = members_sum;

        assert(++cum_sum == _community_cumulative_sizes.end());
        std::cout << "Community member sum: " << members_sum << "\n";
    }


    void LFR::run() {
        _compute_node_distributions();
        _compute_community_size();
        _compute_community_assignments();
        #pragma omp parallel
        #pragma omp single
        {
        #pragma omp task
        _generate_community_graphs();
        #pragma omp task
        _generate_global_graph();
        }
        _merge_community_and_global_graph();
    }

}