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
            for (node_t i = 0; i < static_cast<node_t>(_number_of_nodes); ++i, ++ndd) {
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
        _community_cumulative_sizes.clear();
        _community_cumulative_sizes.reserve(_number_of_communities+1);

        std::random_device rd;
        std::mt19937 gen(rd());

        uint_t needed_memberships = (_number_of_nodes + (_overlap_config.constDegree.overlappingNodes * (_overlap_config.constDegree.multiCommunityDegree - 1)));

        // generate prefix sum of random powerlaw degree distribution
        CommunityDistribution cdd(_community_distribution_params);
        uint_t members_sum = 0;
        community_t min_community = 0;
        community_t min_community_size = std::numeric_limits<community_t>::max();
        for(community_t c = 0; !cdd.empty(); ++cdd, ++c) {
            assert(static_cast<community_t>(_community_cumulative_sizes.size()) == c);

            int_t s = *cdd;
            _community_cumulative_sizes.push_back(s);
            members_sum += s;

            if (s < min_community_size) {
                min_community = c;
                min_community_size = s;
            }

            // remove communities as long as we have too many memberships
            while (members_sum > needed_memberships) {
                std::uniform_int_distribution<> dis(0, c);
                community_t x = dis(gen);
                members_sum -= _community_cumulative_sizes[x];
                std::swap(_community_cumulative_sizes[x], _community_cumulative_sizes.back());
                _community_cumulative_sizes.pop_back();
                --c;
                if (min_community == x) {
                    auto it = std::min_element(_community_cumulative_sizes.begin(), _community_cumulative_sizes.end());
                    min_community_size = *it;
                    min_community = std::distance(_community_cumulative_sizes.begin(), it);
                }
            }
        }

        if (members_sum < needed_memberships) {
            if (static_cast<int_t>(needed_memberships - members_sum) > _community_distribution_params.maxDegree) {
                STXXL_ERRMSG("There are " << (needed_memberships - members_sum) << " memberships missing, which is more than the size of the largest community (" << _community_distribution_params.maxDegree << "), you need to specify more communities!");
                abort();
            }
            _community_cumulative_sizes[min_community] += (needed_memberships - members_sum);
            std::cout << "Added " << (needed_memberships - members_sum) << " memberships to community " << min_community << " which has now size " << _community_cumulative_sizes[min_community] << std::endl;
            members_sum = needed_memberships;
        }

        SEQPAR::sort(_community_cumulative_sizes.begin(), _community_cumulative_sizes.end(), std::greater<decltype(_community_cumulative_sizes)::value_type>());

        assert(static_cast<community_t>(members_sum) ==
               std::accumulate(_community_cumulative_sizes.begin(), _community_cumulative_sizes.end(), 0));

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

        std::cout << "Resulting graph has " << _edges.size() << " edges, " << _intra_community_edges.size() << " of them are intra-community edges and " << _inter_community_edges.size() << " of them are inter-community edges" << std::endl;
    }

}