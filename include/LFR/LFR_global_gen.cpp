#include "LFR.h"
#include "GlobalRewiringSwapGenerator.h"
#include <LFR/GlobalRewiringSwapGenerator.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/EdgeSwapParallelTFP.h>
#include <SwapGenerator.h>
#include <Utils/AsyncStream.h>
#include <Utils/StreamPusher.h>

namespace LFR {
    void LFR::_generate_global_graph(int_t globalSwapsPerIteration) {
        {
            HavelHakimiIMGenerator gen(HavelHakimiIMGenerator::DecreasingDegree);

            int_t degree_sum = 0;

            // FIXME this is not necessary for non-overlapping clusters
            { // push node degrees in descending order in generator
                stxxl::sorter<degree_t, GenericComparator<degree_t>::Descending> extDegree(GenericComparator<degree_t>::Descending(), SORTER_MEM);
                _node_sorter.rewind();
                while (!_node_sorter.empty()) {
                    extDegree.push(_node_sorter->externalDegree(_mixing));
                    ++_node_sorter;
                }

                extDegree.sort();

                while (!extDegree.empty()) {
                    gen.push(*extDegree);
                    degree_sum += *extDegree;
                    ++extDegree;
                }
            }

            gen.generate();

            _inter_community_edges.resize(degree_sum/2);
            auto endIt = stxxl::stream::materialize(gen, _inter_community_edges.begin());
            _inter_community_edges.resize(endIt - _inter_community_edges.begin());

        }



        { // regular edge swaps
            EdgeSwapParallelTFP::EdgeSwapParallelTFP swapAlgo(_inter_community_edges, globalSwapsPerIteration);
            // Generate swaps
            uint_t numSwaps = 10*_inter_community_edges.size();
            SwapGenerator swapGen(numSwaps, _inter_community_edges.size());

            StreamPusher<decltype(swapGen), decltype(swapAlgo)>(swapGen, swapAlgo);
            swapAlgo.run();
        }

        { // rewiring in order to not to generate new intra-community edges
            EdgeSwapInternalSwaps swapAlgo(_inter_community_edges, globalSwapsPerIteration);

            GlobalRewiringSwapGenerator rewiringSwapGenerator(_community_assignments, _inter_community_edges.size());
            rewiringSwapGenerator.pushEdges(stxxl::vector<edge_t>::bufreader_type(_inter_community_edges));
            rewiringSwapGenerator.generate();

            swapAlgo.setUpdatedEdgesCallback([&rewiringSwapGenerator](const std::vector<edge_t> & updatedEdges) {
                rewiringSwapGenerator.pushEdges(stxxl::stream::streamify(updatedEdges.begin(), updatedEdges.end()));
            });

            while (!rewiringSwapGenerator.empty()) {
                int_t numSwaps = 0;
                // Execute all generated swaps. Some edges might not exist in the second round anymore.
                // Then these edges have been part of a swap already so they might not be a problem anymore.
                // If the target edges should still be a problem we will add them again
                while (!rewiringSwapGenerator.empty()) {
                    swapAlgo.push(*rewiringSwapGenerator);
                    ++numSwaps;
                    ++rewiringSwapGenerator;
                }

                if (numSwaps > 0) {
                    STXXL_MSG("Executing global rewiring phase with " << numSwaps << " swaps.");

                    swapAlgo.process_buffer(); // this triggers the callback and thus pushes new edges in the generator
                    rewiringSwapGenerator.generate();
                }
            }

            // flush any swaps that have not been processed yet, writes edges vector
            swapAlgo.run();

        }
    }

}