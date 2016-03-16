#include <GenericComparator.h>
#include "LFR.h"

#include <Utils/SegmentTree.h>

#ifdef __SSE4_2__
#include "nmmintrin.h"
#endif


struct RandomizingSegmentComparator {
    using value_type = std::pair<uint64_t, uint64_t>;

#ifdef __SSE4_2__
    bool operator()(const value_type & a, const value_type & b) const {
        if (UNLIKELY(a.first == b.first)) {
            auto crc_a = _mm_crc32_u32(0x3982a82b, static_cast<uint32_t>(a.second ^ (a.second >> 32)));
            auto crc_b = _mm_crc32_u32(0x3982a82b, static_cast<uint32_t>(b.second ^ (b.second >> 32)));

            return crc_a < crc_b;

        } else {
            return a.first < b.first;
        }
    }

    value_type min_value() const {
        return std::make_pair(std::numeric_limits<uint64_t>::min(), 0x3982a82b);
    }


    value_type max_value() const {
        return std::make_pair(std::numeric_limits<uint64_t>::max(), 0x5d9dcc7f);
    }
#else
    #warning "Need SSE4.2 for fast randomizing comparator"
    uint32_t hash(uint32_t x) {
        // gcc's linear congruential config
        return 1103515245*x + 12345;
    }

    bool operator()(const value_type & a, const value_type & b) const {
        if (UNLIKELY(a.first == b.first)) {
            auto rnd_a = hash(static_cast<uint32_t>(a.second ^ (a.second << 32)));
            auto rnd_b = hash(static_cast<uint32_t>(b.second ^ (b.second << 32)));

            return crc_a < crc_b;

        } else {
            return a.first < b.first;
        }
    }

    value_type min_value() const {
        return std::make_pair(std::numeric_limits<uint64_t>::min(), 0xfc77a683);
    }


    value_type max_value() const {
        return std::make_pair(std::numeric_limits<uint64_t>::max(), 0xdbdbb1e);
    }
#endif
};


namespace LFR {
    void LFR::_compute_community_assignments() {
        using permutable_assignment = std::tuple<node_t, community_t, degree_t>;
        stxxl::sorter<permutable_assignment, GenericComparatorTuple<permutable_assignment>::Ascending>
              permutable_assignment_sorter(GenericComparatorTuple<permutable_assignment>::Ascending(), SORTER_MEM);

        // sort internal degrees
        using internal_degree_t = std::pair<degree_t, node_t>;
        stxxl::sorter<internal_degree_t, GenericComparator<internal_degree_t>::Descending>
              internal_degree_sorter(GenericComparator<internal_degree_t>::Descending(), SORTER_MEM);


        // In a first step we produce a bipartite graph assigning every node to the requested number of communities.
        // This assignment may violate internal degree size and will be fixed during the randomization phase.
        // The assignment itself requires three steps:
        //  1.) A dry-run of the bipartite havel hakimi determines the number of empty slots in every community
        //  2.) This information is used to update the community size and remove completely empty communities
        //  3.) A second havel hakimi run finally assigns every node to its community and compute the number of
        //      neighbors
        {
            {
                // we keep track of all partially assigned communities
                std::vector<degree_t> community_buffer(_community_cumulative_sizes.size() - 1);
                for (size_t i = 0; i < community_buffer.size(); i++) {
                    community_buffer[i] = _community_size(i);
                }
                community_t filled_communities = 0;
                community_t filled_communities_this_iteration = 0;

                // pointer to last community assigned
                auto comm_it = community_buffer.begin();
                auto comm_end = community_buffer.end();

                // perform bipartite havel hakimi (dry-run to compute community usage)
                for (node_t nid = 0; !_node_sorter.empty(); ++_node_sorter, ++nid) {
                    const auto &node_info = *_node_sorter;
                    community_t memberships_unsatisfied = node_info.memberships;

                    if ((community_buffer.size() - filled_communities) < memberships_unsatisfied) {
                        std::cerr << "Cannot assign node " << nid << " with " << memberships_unsatisfied << " requested memberships to the "
                        << (community_buffer.size() - filled_communities) << " remaining communities" << std::endl;
                        abort();
                    }


                    for (; memberships_unsatisfied; --memberships_unsatisfied) {
                        // fetch next non-empty community
                        do {
                            ++comm_it;
                            if (UNLIKELY(comm_it == comm_end)) {
                                comm_end = community_buffer.begin() + (community_buffer.size() - filled_communities);
                                comm_it = community_buffer.begin();
                                filled_communities_this_iteration = 0;

                            }
                        } while (!*comm_it);

                        // consume a community's slot
                        --(*comm_it);

                        // if community is filled, increase counter s.t. it does not have to be checked again
                        if (!*comm_it) {
                            filled_communities++;
                            filled_communities_this_iteration++;
                        }

                        // we assume decreasing community sizes; if a single community is empty, all following have to
                        assert(!filled_communities_this_iteration || !*comm_it);
                    }
                }

                // update community sizes
                uint_t not_assigned = 0;
                for(size_t i=0; i < community_buffer.size(); i++) {
                    std::cout << "Community " << i << " initial size " << _community_size(i) << " left " << community_buffer[i] << std::endl;
                    not_assigned += community_buffer[i];
                    community_buffer[i] = _community_size(i) - community_buffer[i];
                }

                std::sort(community_buffer.begin(), community_buffer.end(), std::greater<degree_t>());

                {
                    size_t sum = 0;
                    community_t comm_id = 0;

                    for (; comm_id < community_buffer.size() && community_buffer[comm_id]; ++comm_id) {
                        _community_cumulative_sizes[comm_id] = sum;
                        sum += community_buffer[comm_id];
                    }
                    _community_cumulative_sizes[comm_id] = sum;

                    _community_cumulative_sizes.resize(comm_id+1);
                }

                std::cout << "Partially filled communities: " << (community_buffer.size() - filled_communities) << std::endl;
                std::cout << "Non-assigned community members: " << not_assigned << std::endl;
            }

            _node_sorter.rewind();

            // compute deterministic assignment (randomized later)
            {
                // we keep track of all partially assigned communities
                std::vector<degree_t> community_buffer(_community_cumulative_sizes.size() - 1);
                for (size_t i = 0; i < community_buffer.size(); i++) {
                    community_buffer[i] = _community_size(i);
                }
                community_t filled_communities = 0;

                // pointer to last community assigned
                auto comm_it = community_buffer.begin();

                // perform bipartite havel hakimi
                for (node_t nid = 0; !_node_sorter.empty(); ++_node_sorter, ++nid) {
                    const auto &node_info = *_node_sorter;
                    community_t memberships_unsatisfied = node_info.memberships;

                    std::cout << "Node " << nid << "(" << node_info.totalInternalDegree(_mixing) << ") goes to ";

                    if ((community_buffer.size() - filled_communities) < memberships_unsatisfied) {
                        std::cerr << "Cannot assign node " << nid << " with " << memberships_unsatisfied << " requested memberships to the "
                        << (community_buffer.size() - filled_communities) << " remaining communities" << std::endl;
                        abort();
                    }

                    degree_t total_internal_degree = 0;

                    while (memberships_unsatisfied) {
                        // fetch next non-empty community
                        do {
                            comm_it++;
                            if (UNLIKELY(comm_it == community_buffer.end())) {
                                comm_it = community_buffer.begin();
                                community_buffer.resize(community_buffer.size() - filled_communities);
                                filled_communities = 0;
                                assert(community_buffer.size());
                            }
                        } while (!*comm_it);

                        community_t comm_id = comm_it - community_buffer.begin();

                        auto intraDegree = node_info.intraCommunityDegree(_mixing, node_info.memberships - memberships_unsatisfied);
                        permutable_assignment_sorter.push(std::make_tuple(nid, comm_id, intraDegree));
                        std::cout << comm_id << " ";

                        // assign to community
                        --memberships_unsatisfied;
                        --(*comm_it);

                        total_internal_degree += _community_size(comm_id) - 1;

                        if (!*comm_it) {
                            filled_communities++;
                        }

                        assert(!filled_communities || !*comm_it);
                    }

                    internal_degree_sorter.push(std::make_pair(total_internal_degree, nid));
                    std::cout << "Node " << nid << " has estimated internal degree of " << total_internal_degree << std::endl;
                }
            }
        }

        // make EM structure available again
        internal_degree_sorter.sort();
        assert(internal_degree_sorter.size() == _number_of_nodes);
        _node_sorter.rewind();

        // randomize assignment
        using segment_id = uint64_t;
        using segment_assignment = std::pair<segment_id, node_t>;

        stxxl::sorter<segment_assignment, RandomizingSegmentComparator>
              segment_sorter(RandomizingSegmentComparator(), SORTER_MEM);

        // assign every node to a segment
        {
            // segment store
            SegmentTree::SegmentTree<decltype(segment_sorter)> segment_tree(segment_sorter);
            segment_id next_segment_id = 0;

            // get some random bits
            std::default_random_engine generator;
            std::uniform_int_distribution<uint64_t> distribution(0, std::numeric_limits<uint64_t>::max());

            uint64_t queries = 0;

            for (node_t nid = 0; !_node_sorter.empty(); ++_node_sorter, ++nid) {
                const auto &node_info = *_node_sorter;
                degree_t intDegree = node_info.totalInternalDegree(_mixing);

                {
                    // if there are node with an higher internal degree than our current vertex
                    // count them and create a segment corresponding to their id
                    uint64_t new_segment_size = 0;
                    for(; UNLIKELY(!internal_degree_sorter.empty() && (*internal_degree_sorter).first >= intDegree); ++internal_degree_sorter) {
                        new_segment_size++;
                    }

                    if (UNLIKELY(new_segment_size)) {
                        segment_tree.add_segment(new_segment_size, next_segment_id++);
                        std::cout << "Add segment of size " << new_segment_size << " for total internal degree >= " << intDegree<< "\n";
                    }
                }


                if (LIKELY(segment_tree.weight() > 0)) {
                    // FIXME: technically, this wont yield a uniform distribution and we should
                    // replace it by a loop ... but practically this does not matter
                    auto rand_int = distribution(generator) % segment_tree.weight();
                    segment_tree.query(rand_int, nid);
                    queries++;
                } else {
                    // TODO: This assignment is illegal, we have to merge communities trying to make it legal ...
                    std::cerr << "This assignment is illegal, we have to merge communities trying to make it legal ..." << std::endl;
                    abort();
                }
            }

            assert(!segment_tree.weight());
            segment_tree.flush();
            segment_sorter.sort();


            internal_degree_sorter.rewind();
            assert(segment_sorter.size() == internal_degree_sorter.size());
        }


        using permutation_pair = std::pair<node_t, node_t>;
        stxxl::sorter<permutation_pair, GenericComparator<permutation_pair>::Ascending>
              permutation(GenericComparator<permutation_pair>::Ascending(), SORTER_MEM);

        // compute permutation
        {
            for(; !internal_degree_sorter.empty(); ++internal_degree_sorter, ++segment_sorter) {
                const auto & to_be_replaced = *internal_degree_sorter;
                const auto & replaced_with = *segment_sorter;

                permutation.push(std::make_pair(to_be_replaced.second, replaced_with.second));
            }

            assert(segment_sorter.empty());
            permutation.sort();
        }

        stxxl::sorter<CommunityAssignment, GenericComparatorStruct<CommunityAssignment>::Ascending>
              assignments(GenericComparatorStruct<CommunityAssignment>::Ascending(), SORTER_MEM);

        permutable_assignment_sorter.sort();
        // shuffle assignments based on permutation
        {
            for(; !permutation.empty(); ++permutation) {
                assert(!permutable_assignment_sorter.empty());
                auto & old_node_id = (*permutation).first;
                auto & new_node_id = (*permutation).second;

                std::cout << old_node_id << " -> " << new_node_id << std::endl;

                for(; !permutable_assignment_sorter.empty() && std::get<0>(*permutable_assignment_sorter) == old_node_id; ++permutable_assignment_sorter) {
                    auto & comm_id = std::get<1>(*permutable_assignment_sorter);
                    auto & int_degree = std::get<2>(*permutable_assignment_sorter);

                    assignments.push(CommunityAssignment(comm_id, int_degree, new_node_id));
                }
            }

            assert(permutable_assignment_sorter.empty());
        }


        assignments.sort();
        _community_assignments.resize(assignments.size());
        stxxl::stream::materialize(assignments, _community_assignments.begin());
    }
}