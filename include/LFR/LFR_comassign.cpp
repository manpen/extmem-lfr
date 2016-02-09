#include "LFR.h"

namespace LFR {
    void LFR::_compute_community_assignments() {
        // keep results (and sort them lexicographically, so edge switches are possible)
        stxxl::sorter<CommunityAssignment, GenericComparatorStruct<CommunityAssignment>::Ascending>
              assignments(GenericComparatorStruct<CommunityAssignment>::Ascending(), SORTER_MEM);

        // we keep track of all partially assigned communities
        std::list<std::pair<community_t, degree_t>> community_buffer;
        size_t next_unbuffer_community = 0;

        #ifndef NDEBUG
        uint_t members_assigned = 0;
        #endif

        // perform bipartit havel hakimi
        for (node_t nid = 0; !_node_sorter.empty(); ++_node_sorter, ++nid) {
            const auto &node_info = *_node_sorter;
            community_t memberships_unsatisfied = node_info.memberships;

            // assign to buffered communities
            auto current_buffered_com = community_buffer.begin();
            while (memberships_unsatisfied) {
                // if buffer is empty, need to fetch new element
                if (UNLIKELY(current_buffered_com == community_buffer.end())) {
                    if (_number_of_communities == next_unbuffer_community) {
                        std::cerr << "Communities exhausted; cannot fit all nodes. " <<
                        #ifndef NDEBUG
                            "Assigned memberships " << members_assigned <<
                        #endif
                        std::endl;
                        abort();
                    }

                    community_buffer.push_back(std::make_pair(next_unbuffer_community,
                                                              _community_size(next_unbuffer_community)));

                    // position iterator on last element (newly added) of
                    current_buffered_com = community_buffer.end();
                    --current_buffered_com;
                    ++next_unbuffer_community;
                }

                auto intraDegree = node_info.intraCommunityDegree(_mixing, node_info.memberships - memberships_unsatisfied);
                // since both sequences are sorted, we know our algo will find a matching iff one exists
                // so, in case the node does not fit, there is no matching and we abort
                {
                    auto com_size = _community_size(current_buffered_com->first);
                    if (intraDegree > com_size) {
                        std::cerr << "Have to assign a node " << nid << " with intra-community-degree " << intraDegree
                        << " to community " << current_buffered_com->first << " of size " << com_size << std::endl;
                        abort();
                    }
                }

                assignments.push(CommunityAssignment(current_buffered_com->first, intraDegree, nid));
                --memberships_unsatisfied;

                // remove buffered community if its full
                if (0 == --current_buffered_com->second) {
                    auto tmp = current_buffered_com++;
                    community_buffer.erase(tmp);
                } else {
                    current_buffered_com++;
                }

                #ifndef NDEBUG
                members_assigned++;
                #endif
            }
        }
        std::cout << "Number of communities only partially filled: " << community_buffer.size() << "\n";
        std::cout << "Number of communities completely empty: " << (_number_of_communities - next_unbuffer_community) << std::endl;

        // update community sizes
        uint_t not_assigned = 0;
        while (!community_buffer.empty()) {
            const auto &front = community_buffer.front();
            not_assigned += front.second;
            _community_cumulative_sizes[front.first+1] -= not_assigned;
            community_buffer.pop_front();
        }
        _community_cumulative_sizes.resize(static_cast<size_t>(next_unbuffer_community)+1);

        assignments.sort();
        _community_assignments.resize(assignments.size());
        stxxl::stream::materialize(assignments, _community_assignments.begin());

    }
}