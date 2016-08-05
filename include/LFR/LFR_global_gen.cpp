#include "LFR.h"
#include "GlobalRewiringSwapGenerator.h"
#include <LFR/GlobalRewiringSwapGenerator.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeSwaps/SemiLoadedEdgeSwapTFP.h>
#include <SwapGenerator.h>
#include <Utils/AsyncStream.h>
#include <Utils/StreamPusher.h>
#include <Utils/IOStatistics.h>

namespace LFR {
    void LFR::_generate_global_graph(int_t globalSwapsPerIteration) {
        {
            using deg_node_t = std::pair<degree_t, node_t>;
            stxxl::sorter<deg_node_t, GenericComparator<deg_node_t>::Descending> extDegree(GenericComparator<deg_node_t>::Descending(), SORTER_MEM);

            HavelHakimiIMGenerator gen(HavelHakimiIMGenerator::DecreasingDegree);

            int_t degree_sum = 0;

            { // push node degrees in descending order in generator
                _node_sorter.rewind();

                for(node_t nid=0; !_node_sorter.empty(); ++_node_sorter, ++nid) {
                    extDegree.push({_node_sorter->externalDegree(_mixing), nid});
                }

                extDegree.sort();

                while (!extDegree.empty()) {
                    degree_t deg = (*extDegree).first;
                    gen.push(deg);
                    degree_sum += deg;
                    ++extDegree;
                }
            }

            gen.generate();

            // FIXME: This is only necessary, if nodes are not sorted by externalDegree (which happens if we apply ceiling!)
            // We may change the ceiling scheme to avoid it. For the moment, this is the more general solution

            // translate source node id's
            stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> edge_sorter1(GenericComparator<edge_t>::Ascending(), SORTER_MEM);
            {
                extDegree.rewind();
                for (node_t i = 0; !gen.empty(); ++gen) {
                    const edge_t &orig_edge = *gen;
                    for (; i < orig_edge.first; ++extDegree, ++i);

                    edge_sorter1.push({orig_edge.second, (*extDegree).second});
                }
            }

            edge_sorter1.sort();
            extDegree.rewind();

            // translate target node id's
            stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> edge_sorter2(GenericComparator<edge_t>::Ascending(), SORTER_MEM);
            {
                for (node_t i = 0; !edge_sorter1.empty(); ++edge_sorter1) {
                    const edge_t &orig_edge = *edge_sorter1;
                    for (; i < orig_edge.first; ++extDegree, ++i);

                    edge_sorter2.push({orig_edge.second, (*extDegree).second});
                }
            }

            edge_sorter2.sort();

            _inter_community_edges.clear();
            StreamPusher<decltype(edge_sorter2), decltype(_inter_community_edges)> (edge_sorter2, _inter_community_edges);
            _inter_community_edges.consume();
        }

        { // regular edge swaps
            EdgeSwapTFP::SemiLoadedEdgeSwapTFP swapAlgo(_inter_community_edges, globalSwapsPerIteration);
            // Generate swaps
            uint_t numSwaps = _inter_community_edges.size();
            SwapGenerator swapGen(numSwaps, _inter_community_edges.size());

            if (0) {
                IOStatistics ios("GlobalGenInitialRand");
                StreamPusher<decltype(swapGen), decltype(swapAlgo)>(swapGen, swapAlgo);
                swapAlgo.run();
            }

            {
                IOStatistics ios("GlobalGenRewire");

                // rewiring in order to not to generate new intra-community edges
                GlobalRewiringSwapGenerator rewiringSwapGenerator(_community_assignments, _inter_community_edges.size());
                _inter_community_edges.rewind();
                rewiringSwapGenerator.pushEdges(_inter_community_edges);
                _inter_community_edges.rewind();
                rewiringSwapGenerator.generate();

                swapAlgo.setUpdatedEdgesCallback([&rewiringSwapGenerator](EdgeSwapTFP::SemiLoadedEdgeSwapTFP::edge_update_sorter_t &updatedEdges) {
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

}
