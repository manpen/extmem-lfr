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
                _node_sorter.push(ndm);
            }
        }

        _node_sorter.sort();
        std::cout << "Degree sum: " << degree_sum << " Membership sum: " << memebership_sum << "\n";
    }


    void LFR::_compute_community_size() {
        _community_cumulative_sizes.resize(_number_of_communities+1);

        // FIXME: We can easily change the direction of the powerlaw distribution generator!

        CommunityDistribution cdd(_community_distribution_params);
        // The very lazy way ...
        std::vector<degree_t> tmp(_number_of_communities);
        for(auto & t : tmp) {
            assert(!cdd.empty());
            t = *cdd;
            ++cdd;
        }
        assert(cdd.empty());

        {
            uint_t memebers_sum = 0;
            auto cdd = tmp.rbegin();
            for (auto dit = _community_cumulative_sizes.begin() + 1; dit != _community_cumulative_sizes.end(); ++cdd, ++dit) {
                memebers_sum += *cdd;
                *dit = memebers_sum;
            }
#ifndef NDEBUG
            std::cout << "Community member sum: " << memebers_sum << "\n";
#endif
        }

    }


    void LFR::run() {
        _compute_node_distributions();
        _compute_community_size();
        _compute_community_assignments();
        _generate_community_graphs();
    }

}