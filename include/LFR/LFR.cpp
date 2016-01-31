#include "LFR.h"

#include <random>

namespace LFR {

    void LFR::_compute_node_distributions() {
        // setup distributions
        NodeDegreeDistribution ndd(_degree_distribution_params);
        std::default_random_engine generator;
        std::geometric_distribution<int> geo_dist(0.3);

        // have fun
        for (uint_t i = 0; i < static_cast<uint_t>(_number_of_nodes); ++i, ++ndd) {
            assert(!ndd.empty());
            auto &degree = *ndd;

            // compute membership
            // FIXME this is just >some< distribution, which one do we really want?
            community_t memberships;
            {
                degree_t internal_degree = static_cast<degree_t>((1.0 - _mixing) * degree);
                do {
                    memberships = internal_degree / geo_dist(generator);
                } while (!memberships || internal_degree / memberships > _max_degree_within_community);
            }

            _node_sorter.push(NodeDegreeMembership(degree, memberships));
        }

        _node_sorter.sort();
    }

    void LFR::_compute_community_size() {
        _community_sizes.resize(_number_of_communities);
        CommunityDistribution cdd(_community_distribution_params);
        stxxl::stream::materialize(cdd, _community_sizes.rbegin(), _community_sizes.rend());
    }




}