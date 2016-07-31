#include "LFRCommunityAssignBenchmark.h"

namespace LFR {
    void LFRCommunityAssignBenchmark::computeDistribution(unsigned int rounds) {
        _lfr._compute_node_distributions();
        _lfr._compute_community_size();
        _lfr._correct_community_sizes();

        auto com_sizes = _lfr._community_cumulative_sizes;

        community_t bins = 100;
        const community_t binsize = (com_sizes.size() + bins - 1) / bins;
        bins = (com_sizes.size() + binsize - 1) / binsize;

        // total capacity in bins
        std::vector<node_t> total_bin_capacity(bins);
        for(size_t c=0; c<com_sizes.size(); c++)
            total_bin_capacity[c / binsize] += com_sizes[c];


        // search group of nodes w/o constraints
        node_t lowest_node = 0;
        const degree_t unconst_bnd = com_sizes.back();
        for(; !_lfr._node_sorter.empty(); ++lowest_node, ++_lfr._node_sorter) {
            const auto &dgm = *_lfr._node_sorter;
            const auto required_size = dgm.totalInternalDegree(_lfr._mixing);
            if (required_size <= unconst_bnd) {
                std::cout << "Node " << lowest_node << " of intra-degree "
                << required_size << " is first unconstrained node, i.e. <= "
                << unconst_bnd << std::endl;
                break;
            }
        }
        _lfr._node_sorter.rewind();

        if (_lfr._number_of_nodes - lowest_node < 10 * bins) {
            std::cerr << "To few unconstraint nodes" << std::endl;
            abort();
        }

        for(unsigned int round = 0; round < rounds; round++) {
            _lfr._compute_community_assignments();

            auto bin_capacity(total_bin_capacity);
            std::vector<unsigned int> hitsl(bins, 0);
            std::vector<unsigned int> hitsu(bins, 0);

            typename decltype(_lfr._community_assignments)::bufreader_type reader(_lfr._community_assignments);
            for(; !reader.empty(); ++reader) {
                const auto &assign = *reader;

                if (assign.node_id < lowest_node) {
                    bin_capacity[assign.community_id / binsize]--;
                } else if (assign.node_id < lowest_node + 5 * bins) {
                    hitsl[assign.community_id / binsize]++;
                } else if (assign.node_id >= _lfr._number_of_nodes - 5 * bins) {
                    hitsu[assign.community_id / binsize]++;
                }
            }

            for(const auto& c : bin_capacity)
                std::cout << c << " ";
            std::cout << "# free" << std::endl;

            for(const auto& c : hitsl)
                std::cout << c << " ";
            std::cout << "# hitsl" << std::endl;

            for(const auto& c : hitsu)
                std::cout << c << " ";
            std::cout << "# hitsu" << std::endl;

            // restore initial setup
            _lfr._community_cumulative_sizes = com_sizes;
            //_lfr._community_assignments.clear();
            _lfr._node_sorter.rewind();
        }
    }
};