#include <GenericComparator.h>
#include "LFR.h"

#include <Utils/SegmentTree.h>


namespace LFR {
    void LFR::_compute_community_assignments() {
        assert(_overlap_method == constDegree && _overlap_config.constDegree.overlappingNodes == 0);
        auto & com_sizes = _community_cumulative_sizes;

        if (_degree_distribution_params.maxDegree * (1.0 - _mixing) >= _community_distribution_params.maxDegree) {
            throw std::runtime_error("Error, the maximum community is too small to fit the node of the highest degree.");
        }

        if (_degree_distribution_params.minDegree * (1.0 - _mixing) >= _community_distribution_params.minDegree) {
            throw std::runtime_error("Error, the minimum community size is too small to fit the node of the lowest degree.");
        }

        // ensure a legal assignment exists and if not merge communities
        // to make one possible
        {
            // extract community sizes
//            com_sizes.reserve(_community_cumulative_sizes.size()-1);
//            for(community_t c = 0; c < _community_cumulative_sizes.size()-1; c++)
//                com_sizes.push_back(_community_size(c));

            bool updated = false;

            community_t cur_community = 0;
            node_t slots_left = com_sizes[0];

            for(node_t nid=0; !_node_sorter.empty(); ++nid, ++_node_sorter) {
                const auto & dgm = *_node_sorter;
                const auto required_size = dgm.totalInternalDegree(_mixing);

                if (!slots_left) {
                    ++cur_community;
                    assert(cur_community < static_cast<community_t>(com_sizes.size()));
                    slots_left = com_sizes[cur_community];
                }

                auto & community_size = com_sizes[cur_community];

                if (UNLIKELY(required_size > community_size)) {
                    std::cerr << "Community " << (cur_community)
                              << " with size " << community_size
                              << " is too small for node " << nid
                              << " with total internal degree of " << required_size << std::endl;

                    while(community_size < required_size) {
                        community_t smallest_community = static_cast<community_t>(com_sizes.size()-1);

                        if (UNLIKELY(smallest_community == cur_community)) {
                            // should not happen
                            std::cerr << "No communities left to merge" << std::endl;
                            abort();
                        }

                        // gather its size and remove it
                        auto smallest_size = com_sizes[smallest_community];
                        com_sizes.pop_back();

                        std::cerr << " merge community " << smallest_community
                                  << " of size " << smallest_size << std::endl;

                        community_size += smallest_size;
                        slots_left += smallest_size;
                    }

                    updated = true;
                }

                slots_left--;
            }

            _node_sorter.rewind();

            if (updated) {
                SEQPAR::sort(com_sizes.begin(), com_sizes.end(), std::greater<node_t>());
            }
        }

        // keep results (and sort them lexicographically, so edge switches are possible)
        stxxl::sorter<CommunityAssignment, GenericComparatorStruct<CommunityAssignment>::Ascending>
              assignments(GenericComparatorStruct<CommunityAssignment>::Ascending(), SORTER_MEM);

        // assign nodes to communities
        {
            // for every community store the number of slots left;
            // we will also regularly remove empty slots to speed up
            // the random selection process
            struct slot_t {
                community_t community_id;
                node_t members_left;
                node_t available_prefix_sum;
            };
            std::vector<slot_t> slots_left;
            community_t slots_empty = 0;
            community_t slots_deleted = 0;

            const community_t number_of_communities = com_sizes.size();
            slots_left.reserve(number_of_communities);
            for(community_t c=0; c < number_of_communities; c++) {
                assert(com_sizes[c]);
                slots_left.push_back(slot_t {.community_id =  c, .members_left = com_sizes[c], .available_prefix_sum = com_sizes[c]});
            }

            // find the smallest legal community and then uniformly
            // select it or larger one
            community_t largest_illegal_com = 0;

            static_assert(sizeof(community_t) == 4, "Use a 64-bit PRNG");
            stxxl::random_number32 randGen;

            node_t total_sizes_to_choose_from = 0;
            node_t empty_sizes_sum = 0;

            for(node_t nid=0; !_node_sorter.empty(); ++nid, ++_node_sorter) {
                const auto &dgm = *_node_sorter;
                const auto required_size = dgm.totalInternalDegree(_mixing);

                for(; largest_illegal_com<number_of_communities; ++largest_illegal_com) {
                    if (com_sizes[largest_illegal_com] < required_size)
                        break;
                    total_sizes_to_choose_from += com_sizes[largest_illegal_com];
                    if (LIKELY(largest_illegal_com > 0)) {
                        slots_left[largest_illegal_com].available_prefix_sum += slots_left[largest_illegal_com - 1].available_prefix_sum;
                    }
                }

                // compact slots_left if there are too many empty slots
                // there are too many empty slots when half of the sizes are empty
                if(2*empty_sizes_sum > total_sizes_to_choose_from) {
                    auto reader = slots_left.begin() + std::min<community_t>(largest_illegal_com, com_sizes.size()-1);
                    auto writer = reader;

                    const auto begin = slots_left.begin() + slots_deleted;

                    slots_deleted += slots_empty;
                    total_sizes_to_choose_from -= empty_sizes_sum;

                    node_t current_prefix_sum = total_sizes_to_choose_from;

                    while(1) {
                        if (reader->members_left) {
                            *writer = *reader;
                            writer->available_prefix_sum = current_prefix_sum;
                            current_prefix_sum -= com_sizes[writer->community_id];
                            --writer;
                        } else {
                            --slots_empty;
                            empty_sizes_sum -= com_sizes[reader->community_id];
                        }

                        if (reader == begin)
                            break;

                        --reader;
                    }

                    assert(!empty_sizes_sum);
                    assert(!slots_empty);
                    assert(current_prefix_sum == 0);
                }

                // select legal community weighted by (original) community size
                while(1) {
                    assert(largest_illegal_com-slots_deleted > 0);
                    node_t slot_pos = randGen(total_sizes_to_choose_from);
                    auto & slot = *std::lower_bound(slots_left.begin() + slots_deleted, slots_left.begin() + largest_illegal_com, slot_pos, 
                            [](const slot_t& a, node_t b) { return a.available_prefix_sum < b; }
                            );

                    if (LIKELY(slot.members_left)) {
                        // found a non-empty community
                        assignments.push(CommunityAssignment(slot.community_id, required_size, nid));
                        //std::cout << "Assing node " << nid << " with degree " << required_size << " to community " << slot.first << " with " << (slot.second - 1) << " left" << std::endl;

                        --slot.members_left;
                        if (!slot.members_left) {
                            slots_empty++;
                            empty_sizes_sum += com_sizes[slot.community_id];
                        }

                        break;
                    }
                }
            }
        }

        assignments.sort();
        _community_assignments.resize(assignments.size());
        stxxl::stream::materialize(assignments, _community_assignments.begin(), _community_assignments.end());

        {
            _community_cumulative_sizes.push_back(0);

            // exclusive prefix sum
            community_t sum = 0;
            for (size_t c = 0; c < _community_cumulative_sizes.size(); ++c) {
                community_t tmp = _community_cumulative_sizes[c];
                _community_cumulative_sizes[c] = sum;
                sum += tmp;
            }
        }

        assert(_community_cumulative_sizes.back() == _number_of_nodes);

#ifndef NDEBUG
        // check that assignment is valid
        {
            community_t com = 0;
            node_t size = 0;
            degree_t max_deg = 0;

            for (typename decltype(_community_assignments)::bufreader_type reader(_community_assignments);
                 !reader.empty(); ++reader) {

                auto & a = *reader;

                if (a.community_id == com) {
                    size++;
                    max_deg = std::max<degree_t>(max_deg, a.degree);
                } else {
                    assert(size == _community_size(com));
                    assert(size >= max_deg);
                    com = a.community_id;
                    size = 1;
                    max_deg = a.degree;
                }
            }

            assert(size == _community_size(com));
            assert(size >= max_deg);
        }
#endif
    }
}
