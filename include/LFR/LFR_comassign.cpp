#include "LFR.h"

namespace LFR {
    void LFR::_compute_community_assignments() {
        // keep results (and sort them lexicographically, so edge switches are possible)
        stxxl::sorter<CommunityAssignment, GenericComparatorStruct<CommunityAssignment>::Ascending>
              assignments(GenericComparatorStruct<CommunityAssignment>::Ascending(), SORTER_MEM);

        // we keep track of all partially assigned communities
        std::list<std::pair<community_t, degree_t>> community_buffer;
        size_t next_unbuffer_community = 0;

        // perform bipartit havel hakimi
        for (node_t nid = 0; !_node_sorter.empty(); ++_node_sorter, ++nid) {
            const auto &node_info = *_node_sorter;
            community_t memberships_unsatisfied = node_info.memberships;

            // assign to buffered communities
            auto com = community_buffer.begin();
            while (memberships_unsatisfied) {
                // if buffer is empty, need to fetch new element
                if (UNLIKELY(com == community_buffer.end())) {
                    if (_community_sizes.size() == next_unbuffer_community) {
                        std::cerr << "Communities exhausted; cannot fit all nodes";
                        abort();
                    }

                    community_buffer.push_back(std::make_pair(next_unbuffer_community, _community_sizes[next_unbuffer_community]));

                    // position iterator on last element (newly added) of
                    com = community_buffer.end();
                    --com;
                    ++next_unbuffer_community;
                }

                // since both sequences are sorted, we know our algo will find a matching iff one exists
                // so, in case the node does not fit, there is no matching and we abort
                if (node_info.intraCommunityDegree(_mixing) > _community_sizes[com->first]) {
                    std::cerr << "Have to assign a node " << nid << " with intra-community-degree " << node_info.intraCommunityDegree(_mixing)
                    << " to community " << com->first << " of size " << _community_sizes[com->first] << std::endl;
                    abort();
                }

                assignments.push(CommunityAssignment(com->first, nid));
                --memberships_unsatisfied;

                // remove buffered community if its full
                if (0 == --com->second) {
                    auto tmp = com++;
                    community_buffer.erase(tmp);
                } else {
                    com++;
                }
            }
        }
        std::cout << "Number of communities only partially filled: " << community_buffer.size() << "\n";
        std::cout << "Number of communities completely empty: " << (_community_sizes.size() - next_unbuffer_community) << std::endl;

        // update community sizes
        while (!community_buffer.empty()) {
            const auto &front = community_buffer.front();
            _community_sizes[front.first] -= front.second;
            community_buffer.pop_front();
        }
        _community_sizes.resize(static_cast<size_t>(next_unbuffer_community));
    }
}