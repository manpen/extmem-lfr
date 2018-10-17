#include "LFR.h"
#include "GlobalRewiringSwapGenerator.h"
#include <LFR/GlobalRewiringSwapGenerator.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeSwaps/SemiLoadedEdgeSwapTFP.h>
#include <SwapGenerator.h>
#include <Utils/AsyncStream.h>
#include <Utils/StreamPusher.h>
#include <Utils/IOStatistics.h>
#include <DegreeStream.h>
#include <Utils/NodeHash.h>
#include <Curveball/EMCurveball.h>
#include <Utils/RandomSeed.h>

namespace LFR {
    void LFR::_generate_global_graph(int_t globalSwapsPerIteration) {
		HavelHakimiIMGenerator gen(HavelHakimiIMGenerator::DecreasingDegree);

        #ifdef CURVEBALL_RAND
        DegreeStream rewindable_ext_degrees;
        #endif

		{
            using deg_node_t = std::pair<degree_t, node_t>;
            stxxl::sorter<deg_node_t, GenericComparator<deg_node_t>::Descending> extDegree(GenericComparator<deg_node_t>::Descending(), SORTER_MEM);

            int_t degree_sum = 0;

            { // push node degrees in descending order in generator
                _node_sorter.rewind();

                for(node_t nid=0; !_node_sorter.empty(); ++_node_sorter, ++nid) {
                    extDegree.push({_node_sorter->externalDegree(_mixing), nid});
                    #ifdef CURVEBALL_RAND
                    rewindable_ext_degrees.push(_node_sorter->externalDegree(_mixing));
                    #endif
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

            // translate target node id's
            // the sorter is in the outer scope as it is needed for longer
            stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> edge_sorter2(GenericComparator<edge_t>::Ascending(), SORTER_MEM);

            {
                // translate source node id's
                stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> edge_sorter1(GenericComparator<edge_t>::Ascending(), SORTER_MEM);

                extDegree.rewind();
                for (node_t i = 0; !gen.empty(); ++gen) {
                    const edge_t &orig_edge = *gen;
                    for (; i < orig_edge.first; ++extDegree, ++i);

                    edge_sorter1.push({orig_edge.second, (*extDegree).second});
                }

                edge_sorter1.sort();
                extDegree.rewind();

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

        std::cout << "Current EM allocation after InitialGlobalGen: " <<  stxxl::block_manager::get_instance()->get_current_allocation() << std::endl;
        std::cout << "Maximum EM allocation after InitialGlobalGen: " <<  stxxl::block_manager::get_instance()->get_maximum_allocation() << std::endl;

        {
			#ifdef CURVEBALL_RAND
				//gen.finalize();
				//DegreeStream& degs = gen.get_degree_stream();
				//degs.rewind();
				rewindable_ext_degrees.rewind();

				Curveball::EMCurveball<Curveball::ModHash> randAlgo(_inter_community_edges,
                                                                    rewindable_ext_degrees,
                                                                    _number_of_nodes,
                                                                    20,
                                                                    _inter_community_edges,
                                                                    omp_get_max_threads(),
                                                                    _max_memory_usage,
                                                                    true); // change to false when it is possible

                if (1) {
                    IOStatistics ios("GlobalGenInitialRandCurveball");
                    randAlgo.run();
                    _inter_community_edges.rewind();
                }

                // regular edge swaps in the rewiring
				EdgeSwapTFP::SemiLoadedEdgeSwapTFP swapAlgo(_inter_community_edges, globalSwapsPerIteration, _number_of_nodes, _max_memory_usage);
            #else
				// regular edge swaps
				EdgeSwapTFP::SemiLoadedEdgeSwapTFP swapAlgo(_inter_community_edges, globalSwapsPerIteration, _number_of_nodes, _max_memory_usage);
				// Generate swaps
				uint_t numSwaps = 10*_inter_community_edges.size();
				SwapGenerator swapGen(numSwaps, _inter_community_edges.size(), RandomSeed::get_instance().get_next_seed());

				if (1) {
					IOStatistics ios("GlobalGenInitialRand");
					StreamPusher<decltype(swapGen), decltype(swapAlgo)>(swapGen, swapAlgo);
					swapAlgo.run();
				}
			#endif

            std::cout << "Current EM allocation after GlobalGenInitialRand: " <<  stxxl::block_manager::get_instance()->get_current_allocation() << std::endl;
            std::cout << "Maximum EM allocation after GlobalGenInitialRand: " <<  stxxl::block_manager::get_instance()->get_maximum_allocation() << std::endl;

            {
                IOStatistics ios("GlobalGenRewire");

                // rewiring in order to not to generate new intra-community edges
                GlobalRewiringSwapGenerator rewiringSwapGenerator(_community_assignments, _inter_community_edges.size(), RandomSeed::get_instance().get_next_seed());
                _inter_community_edges.rewind();
                rewiringSwapGenerator.pushEdges(_inter_community_edges);
                _inter_community_edges.rewind();
                rewiringSwapGenerator.generate();

                std::cout << "Current EM allocation after GlobalGenRewireInit: " <<  stxxl::block_manager::get_instance()->get_current_allocation() << std::endl;
                std::cout << "Maximum EM allocation after GlobalGenRewireInit: " <<  stxxl::block_manager::get_instance()->get_maximum_allocation() << std::endl;

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
