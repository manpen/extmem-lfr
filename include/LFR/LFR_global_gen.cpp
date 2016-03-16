#include "LFR.h"
#include "GlobalRewiringSwapGenerator.h"
#include <LFR/GlobalRewiringSwapGenerator.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <SwapGenerator.h>
#include <Utils/AsyncStream.h>

namespace LFR {
    void LFR::_generate_global_graph() {
        HavelHakimiIMGenerator gen(HavelHakimiIMGenerator::DecreasingDegree);

        int_t degree_sum = 0;

        // FIXME find out if this sorting is necessary at all or if _node_sorter already sorts in the desired order
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

        // FIXME this sort might also be eliminated if edges are produced in the right order
        { // writes edges sorted in _inter_community_edges
            stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> intra_edgeSorter(GenericComparator<edge_t>::Ascending(), SORTER_MEM);

            _inter_community_edges.resize(degree_sum/2);

            while (!gen.empty()) {
                edge_t e(gen->first, gen->second);
                e.normalize();
                intra_edgeSorter.push(e);

                ++gen;
            }

            intra_edgeSorter.sort();
            auto endIt = stxxl::stream::materialize(intra_edgeSorter, _inter_community_edges.begin());

            _inter_community_edges.resize(endIt - _inter_community_edges.begin());
        }


        int_t swapsPerIteration = _inter_community_edges.size()/3;

        EdgeSwapInternalSwaps swapAlgo(_inter_community_edges, swapsPerIteration);

        { // regular edge swaps
            // Generate swaps
            uint_t numSwaps = 10*_inter_community_edges.size();
            SwapGenerator swapGen(numSwaps, _inter_community_edges.size());

            // perform swaps
            AsyncStream<SwapGenerator> astream(swapGen, true, swapsPerIteration, 2);

            for(; !astream.empty(); astream.nextBuffer())
                swapAlgo.swap_buffer(astream.readBuffer());
            swapAlgo.flush();
        }

        { // rewiring in order to not to generate new intra-community edges
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
        }

        // flush any swaps that have not been processed yet, writes edges vector
        swapAlgo.run();
    }

}