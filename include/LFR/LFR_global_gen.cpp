#include "LFR.h"
#include "GlobalRewiringSwapGenerator.h"
#include <LFR/GlobalRewiringSwapGenerator.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeSwaps/SemiLoadedEdgeSwapTFP.h>
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

            _inter_community_edges.clear();
            StreamPusher<decltype(gen), decltype(_inter_community_edges)> (gen, _inter_community_edges);
            _inter_community_edges.consume();
        }



        { // regular edge swaps
            EdgeSwapTFP::SemiLoadedEdgeSwapTFP swapAlgo(_inter_community_edges, globalSwapsPerIteration);
            // Generate swaps
            uint_t numSwaps = _inter_community_edges.size()/100;
            SwapGenerator swapGen(numSwaps, _inter_community_edges.size());

            StreamPusher<decltype(swapGen), decltype(swapAlgo)>(swapGen, swapAlgo);
            swapAlgo.run();

            // rewiring in order to not to generate new intra-community edges
            GlobalRewiringSwapGenerator rewiringSwapGenerator(_community_assignments, _inter_community_edges.size());
            _inter_community_edges.rewind();
            rewiringSwapGenerator.pushEdges(_inter_community_edges);
            _inter_community_edges.rewind();
            rewiringSwapGenerator.generate();

            swapAlgo.setUpdatedEdgesCallback([&rewiringSwapGenerator](EdgeSwapTFP::SemiLoadedEdgeSwapTFP::edge_update_sorter_t & updatedEdges) {
                rewiringSwapGenerator.pushEdges(updatedEdges);
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

                    swapAlgo.process_swaps(); // this triggers the callback and thus pushes new edges in the generator
                    rewiringSwapGenerator.generate();
                }
            }

            // flush any swaps that have not been processed yet, writes edges vector
            swapAlgo.run();

        }
    }

}
