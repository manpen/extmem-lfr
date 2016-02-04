#include "LFR.h"

#include <random>

namespace LFR {

    void LFR::_compute_node_distributions() {
        // setup distributions
        NodeDegreeDistribution ndd(_degree_distribution_params);
        std::default_random_engine generator;
        std::geometric_distribution<int> geo_dist(0.1);

        #ifndef NDEBUG
        uint_t degree_sum = 0;
        uint_t memebership_sum = 0;
        #endif

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
                    auto r = (1+geo_dist(generator));
                    memberships = internal_degree / r;
                } while (
                    !memberships ||
                    memberships > 8*_community_distribution_params.numberOfNodes / 10 ||
                    internal_degree / memberships > _max_degree_within_community
                );
            }

            _node_sorter.push(NodeDegreeMembership(degree, memberships));
            #ifndef NDEBUG
            degree_sum += degree;
            memebership_sum += memberships;
            #endif
        }

        _node_sorter.sort();
        #ifndef NDEBUG
        std::cout << "Degree sum: " << degree_sum << " Membership sum: " << memebership_sum << "\n";
        #endif
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