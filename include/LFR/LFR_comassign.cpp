#include <GenericComparator.h>
#include "LFR.h"

#include <Utils/RandomIntervalTree.h>


namespace LFR {
// ensure a legal assignment exists and if not merge communities
// to make one possible
void LFR::_correct_community_sizes() {
    auto & com_sizes = _community_cumulative_sizes;

    if (_degree_distribution_params.maxDegree * (1.0 - _mixing) >= _community_distribution_params.maxDegree) {
        throw std::runtime_error("Error, the maximum community is too small to fit the node of the highest degree.");
    }

    if (_degree_distribution_params.minDegree * (1.0 - _mixing) >= _community_distribution_params.minDegree) {
        throw std::runtime_error("Error, the minimum community size is too small to fit the node of the lowest degree.");
    }

    bool updated = false;

    community_t cur_community = 0;
    struct Slot {
        node_t capacity;
        node_t free;
        community_t community_id;
        Slot(const node_t& c, const node_t&f, const community_t& co) :
              capacity(c), free(f), community_id(co)
        {};
    };
    std::list<Slot> slots_left;
    slots_left.emplace_back(com_sizes[cur_community], com_sizes[cur_community], 0);

    for(node_t nid=0; !_node_sorter.empty(); ++nid, ++_node_sorter) {
        const auto & dgm = *_node_sorter;

        auto slot = slots_left.begin();

        bool merged = false;
        for(community_t mem=0; mem<dgm.memberships(); mem++) {
            const auto required_size = dgm.intraCommunityDegree(_mixing, mem);

            if (slot == slots_left.end()) {
                if (required_size <= _community_distribution_params.maxDegree && !slots_left.empty()) {
                    // disable disjoint community test, since the node is reasonable small to
                    // later on swap it successfully
                    slot = slots_left.begin();
                } else {
                    ++cur_community;
                    if (cur_community >= static_cast<community_t>(com_sizes.size())) {
                        auto size = _number_of_nodes - nid;
                        std::cerr << "Add community of size " << size << std::endl;
                        com_sizes.push_back(size);

                        updated = true;
                    }
                    slots_left.emplace_back(com_sizes[cur_community], com_sizes[cur_community], cur_community);
                    slot = std::prev(slots_left.end());
                }
            }

            if (UNLIKELY(required_size > slot->capacity)) {
                std::cerr << "Community " << (cur_community)
                << " with size " << slot->capacity
                << " is too small for node " << nid
                << " with total internal degree of " << required_size << std::endl;

                while(slot->capacity < required_size) {
                    community_t smallest_community = static_cast<community_t>(com_sizes.size()-1);

                    if (UNLIKELY(smallest_community == cur_community)) {
                        // should not happen
                        std::cerr << "No communities left to merge; just increase weight" << std::endl;
                        abort();
                    } else {
                        // gather its size and remove it
                        auto smallest_size = com_sizes[smallest_community];
                        com_sizes.pop_back();

                        std::cerr << " merge community " << smallest_community
                        << " of size " << smallest_size << std::endl;

                        slot->capacity += smallest_size;
                        slot->free += smallest_size;
                    }
                }

                updated = true;
            }

            if (!--slot->free) {
                slot = slots_left.erase(slot);
            } else {
                ++slot;
            }
        }

        if (merged) {
            slots_left.sort([] (const Slot&a, const Slot&b) {
                return a.capacity > b.capacity;
            });
        }
    }

    _node_sorter.rewind();

    for(const auto& slot : slots_left)
        com_sizes[slot.community_id] -= slot.free;

    if (updated) {
        SEQPAR::sort(com_sizes.begin(), com_sizes.end(), std::greater<node_t>());
    }

#ifndef NDEBUG
    std::cout << "Community member sum after correction: " << std::accumulate(com_sizes.begin(), com_sizes.end(), 0) << std::endl;
#endif
}

void LFR::_compute_community_assignments() {
    auto & com_sizes = _community_cumulative_sizes;

    // keep results (and sort them lexicographically, so edge switches are possible)
    stxxl::sorter<CommunityAssignment, GenericComparatorStruct<CommunityAssignment>::Ascending>
          assignments(GenericComparatorStruct<CommunityAssignment>::Ascending(), SORTER_MEM);


    const node_t offline_alloc = (_overlap_max_memberships == 1) ? 0 : std::min(1024*1024, _number_of_nodes / 10);
    const node_t online_alloc = _number_of_nodes - offline_alloc;

    std::cout << "Will try to assign "
              << online_alloc << " nodes with online allocation and "
              << offline_alloc << " nodes with offline allocation "
              << std::endl;


    // assign nodes to communities in an streaming fashion
    edgeid_t membership_sum = 0;

    RandomIntervalTree<node_t> tree(com_sizes);
    const community_t number_of_communities = com_sizes.size();
    stxxl::random_number64 randGen;

    // find the smallest legal community and then uniformly
    // select it or larger one
    node_t legal_weight = 0;
    community_t largest_illegal_com = 0;
    degree_t current_degree = std::numeric_limits<degree_t>::max();

    auto update_legal_weight = [&] (const degree_t &new_degree) {
        if (LIKELY(new_degree == current_degree))
            return;

        if (new_degree > current_degree) {
            while(--largest_illegal_com) {
                if (com_sizes[largest_illegal_com] > new_degree) {
                    ++largest_illegal_com;
                    break;
                }
            }

        } else {
            for(;largest_illegal_com < number_of_communities; ++largest_illegal_com) {
                if (com_sizes[largest_illegal_com] <= new_degree) {
                    break;
                }
            }

        }

        assert(largest_illegal_com > 0);
        assert(new_degree < com_sizes.at(largest_illegal_com - 1));
        assert(largest_illegal_com == number_of_communities || new_degree >= com_sizes.at(largest_illegal_com));

        legal_weight = tree.prefixsum(largest_illegal_com - 1);
        current_degree = new_degree;
    };


    std::set<community_t> communities;
    for (node_t nid = 0; nid < online_alloc ; ++nid, ++_node_sorter) {
        assert(!_node_sorter.empty());
        const auto &dgm = *_node_sorter;

        communities.clear();

        membership_sum += dgm.memberships();
        for (community_t mem = 0; mem < dgm.memberships(); mem++) {
            const auto required_size = dgm.intraCommunityDegree(_mixing, mem);
            update_legal_weight(required_size);
            assert(legal_weight <= tree.total_weight());
            assert(legal_weight > 0);

            // select legal community weighted by community size
            unsigned int retries = 100 * dgm.memberships();
            while (1) {
                community_t community_selected = tree.getLeaf(randGen(legal_weight));
                assert(community_selected < largest_illegal_com);

                if (communities.insert(community_selected).second) {
                    // found a non-empty community
                    assert(required_size <= com_sizes.at(community_selected));
                    assignments.push(CommunityAssignment(community_selected, required_size, nid));
                    tree.decreaseLeaf(community_selected);
                    //com_sizes[community_selected]++;
                    --legal_weight;
                    break;
                }

                if (UNLIKELY(!--retries)) {
                    std::cerr << "Failed to assigned node " << nid
                        << " to its " << mem << " of " << dgm.memberships()
                        << " memberships. Start over." << std::endl;
                    for(auto& c : communities)
                        std::cerr << c << " ";
                    std::cerr << std::endl;

                    _node_sorter.rewind();
                    _compute_community_assignments();
                    return;
                }
            }
        }
    }

    // offline allocation
    if (offline_alloc) {
        // load data into IM structures
        std::vector<NodeDegreeMembership> ndgs;
        std::vector<node_t> membership_offsets;
        ndgs.reserve(offline_alloc);
        membership_offsets.reserve(offline_alloc);

        node_t membership_no = 0;
        for(node_t nid=online_alloc; nid < _number_of_nodes; ++_node_sorter, ++nid) {
            assert(!_node_sorter.empty());
            const auto & ndm = *_node_sorter;
            ndgs.push_back(ndm);
            membership_offsets.push_back(membership_no);
            membership_no += ndm.memberships();
        }
        assert(_node_sorter.empty());
        std::vector<CommunityAssignment> off_assignments;
        off_assignments.reserve(membership_no);

        uint64_t swap_tests = 0;
        uint64_t successful_swaps = 0;


        // allocate
        {
            std::set<community_t> communities;
            for (node_t i = 0; i < offline_alloc; i++) {
                const auto &dgm = ndgs.at(i);
                communities.clear();

                membership_sum += dgm.memberships();
                for (community_t mem = 0; mem < dgm.memberships(); mem++) {
                    const auto required_size = dgm.intraCommunityDegree(_mixing, mem);
                    update_legal_weight(required_size);
                    assert(legal_weight <= tree.total_weight());
                    assert(legal_weight > 0);

                    // select legal community weighted by community size
                    unsigned int retries = 100 * dgm.memberships();
                    while (1) {
                        community_t community_selected = tree.getLeaf(randGen(legal_weight));
                        assert(community_selected < largest_illegal_com);

                        if (communities.insert(community_selected).second) {
                            off_assignments.emplace_back(community_selected, required_size, i);
                            tree.decreaseLeaf(community_selected);
                            --legal_weight;

                            break;
                        }

                        // try to find a node with which we can swap a community
                        bool assigned = false;
                        for(unsigned int r=0; !assigned && r < 10*dgm.memberships(); r++) {
                            ++swap_tests;

                            auto & other = off_assignments[randGen(off_assignments.size())];
                            if (UNLIKELY(other.node_id == i))
                                // dont want to switch with ourselves ;)
                                continue;

                            if (other.degree > com_sizes[community_selected])
                                // node constraints are violated in our selected comm
                                continue;

                            // check whether other node is already assigned to our community
                            auto begin = off_assignments.cbegin() + membership_offsets.at(other.node_id);
                            auto end = off_assignments.cbegin() + membership_offsets.at(other.node_id+1);
                            if (end == std::find_if(begin, end, [community_selected] (const CommunityAssignment& as) {
                                return as.community_id == community_selected;
                            })) {
                                continue;
                            }

                            // check whether we are already in its community
                            if (!communities.insert(other.community_id).second)
                                continue;

                            ++successful_swaps;

                            // finally swap communities and assign
                            tree.decreaseLeaf(community_selected);
                            --legal_weight;

                            std::swap(other.community_id, community_selected);
                            off_assignments.emplace_back(community_selected, required_size, i);

                            assigned = true;
                        }
                        if (assigned)
                            break;

                        if (UNLIKELY(!--retries)) {
                            std::cerr << "Failed to assigned node " << (i + online_alloc)
                            << " to its " << mem << " of " << dgm.memberships()
                            << " memberships. Start over." << std::endl;
                            for (auto &c : communities)
                                std::cerr << c << " ";
                            std::cerr << std::endl;

                            _node_sorter.rewind();
                            _compute_community_assignments();
                            return;
                        }
                    }
                }
            }
        }

        std::cout << "Performed " << swap_tests << " (" << (static_cast<double>(swap_tests) / offline_alloc) << " per node) swap tests of which "
                  << successful_swaps << " (" << (100.0 * successful_swaps / swap_tests) << " %) were successful."
                  << std::endl;

        // push im structure to em
        for(auto & a : off_assignments) {
            assert(a.degree <= com_sizes.at(a.community_id));
            a.node_id += online_alloc;
            assignments.push(a);
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

    assert(_community_cumulative_sizes.back() == membership_sum);
}
}