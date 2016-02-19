#include "LFR.h"
#include "GlobalRewiringSwapGenerator.h"
#include <HavelHakimi/HavelHakimiGenerator.h>
#include <HavelHakimi/HavelHakimiGeneratorRLE.h>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <DistributionCount.h>
#include <SwapGenerator.h>
#include <stxxl/vector>
#include <stxxl/sorter>
#include <IMGraph.h>
#include <EdgeSwaps/IMEdgeSwap.h>
#include <Utils/AsyncStream.h>

namespace LFR {
    void LFR::_generate_community_graphs() {
        decltype(_community_assignments)::bufreader_type assignment_reader(_community_assignments);
        std::vector<node_t> node_ids;
        std::vector<degree_t> node_degrees;
        stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> edgeSorter(GenericComparator<edge_t>::Ascending(), SORTER_MEM);
        stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> intra_edgeSorter(GenericComparator<edge_t>::Ascending(), SORTER_MEM);

        for (community_t i = 0; i < static_cast<community_t>(_community_cumulative_sizes.size()) - 1; ++i) {
            node_t com_size = _community_cumulative_sizes[i+1] - _community_cumulative_sizes[i];
            // FIXME: implement this with EM vectors for large communities!
            node_ids.clear();
            node_ids.reserve(com_size);
            node_degrees.clear();
            node_degrees.reserve(com_size);

            while (!assignment_reader.empty() && assignment_reader->community_id == i) {
                node_degrees.push_back(assignment_reader->degree);
                node_ids.push_back(assignment_reader->node_id);
                ++assignment_reader;
            }

            if (com_size < 2) continue; // no edges to create

            auto node_deg_stream = stxxl::stream::streamify(node_degrees.begin(), node_degrees.end());

            HavelHakimiPrioQueueInt prio_queue;
            std::stack<HavelHakimiNodeDegree> stack;
            HavelHakimiGenerator<HavelHakimiPrioQueueInt, std::stack<HavelHakimiNodeDegree>> gen{prio_queue, stack, node_deg_stream};

            if (gen.maxEdges() < 1 * IntScale::M) { // TODO: make limit configurable!
                IMGraph graph(node_degrees);
                while (!gen.empty()) {
                    graph.addEdge(*gen);
                    ++gen;
                }

                if (graph.numEdges() > 1) {
                    // Generate swaps
                    uint_t numSwaps = 10*graph.numEdges();

                    IMEdgeSwap swapAlgo(graph);
                    for (SwapGenerator swapGen(numSwaps, graph.numEdges()); !swapGen.empty(); ++swapGen) {
                        swapAlgo.push(*swapGen);
                    }

                    swapAlgo.run();
                }

                for (auto it = graph.getEdges(); !it.empty(); ++it) {
                    edge_t e = {node_ids[it->first], node_ids[it->second]};
                    e.normalize();
                    edgeSorter.push(e);
                }
            } else {
                stxxl::vector<edge_t> intra_edges(gen.maxEdges());

                while (!gen.empty()) {
                    if (gen->first < gen->second) {
                        intra_edgeSorter.push(edge_t {gen->first, gen->second});
                    } else {
                        intra_edgeSorter.push(edge_t {gen->second, gen->first});
                    }

                    ++gen;
                }

                intra_edgeSorter.sort();
                auto endIt = stxxl::stream::materialize(intra_edgeSorter, intra_edges.begin());
                intra_edgeSorter.clear();

                intra_edges.resize(endIt - intra_edges.begin());

                // Generate swaps
                uint_t numSwaps = 10*intra_edges.size();
                SwapGenerator swapGen(numSwaps, intra_edges.size());
                stxxl::vector<SwapDescriptor> swaps(numSwaps);
                stxxl::stream::materialize(swapGen, swaps.begin());

                // perform swaps
                EdgeSwapInternalSwaps swapAlgo(intra_edges, swaps, intra_edges.size()/3);
                swapAlgo.run();

                decltype(intra_edges)::bufreader_type edge_reader(intra_edges);
                while (!edge_reader.empty()) {
                    edge_t e = {node_ids[edge_reader->first], node_ids[edge_reader->second]};
                    e.normalize();
                    edgeSorter.push(e);
                    ++edge_reader;
                }
            }
        }

        edgeSorter.sort();

        {
            // TODO if we assume that the maximum degree fits in RAM, the distribution counting could be done using bucket sort in IM.
            stxxl::sorter<degree_t, GenericComparator<degree_t>::Descending> extDegree(GenericComparator<degree_t>::Descending(), SORTER_MEM);
            _node_sorter.rewind();
            while (!_node_sorter.empty()) {
                extDegree.push(_node_sorter->externalDegree(_mixing));
                ++_node_sorter;
            }

            extDegree.sort();

            DistributionCount<decltype(extDegree)> dcount(extDegree);
            HavelHakimiGeneratorRLE<decltype(dcount)> gen(dcount);
            stxxl::vector<edge_t> external_edges(gen.maxEdges());

            while (!gen.empty()) {
                if (gen->first < gen->second) {
                    intra_edgeSorter.push(edge_t {gen->first, gen->second});
                } else {
                    intra_edgeSorter.push(edge_t {gen->second, gen->first});
                }

                ++gen;
            }

            intra_edgeSorter.sort();
            auto endIt = stxxl::stream::materialize(intra_edgeSorter, external_edges.begin());
            intra_edgeSorter.clear();

            external_edges.resize(endIt - external_edges.begin());

            int_t swapsPerIteration = external_edges.size()/3;

            EdgeSwapInternalSwaps swapAlgo(external_edges, swapsPerIteration);

            {
                // Generate swaps
                uint_t numSwaps = 10*external_edges.size();
                SwapGenerator swapGen(numSwaps, external_edges.size());

                // perform swaps
                AsyncStream<SwapGenerator> astream(swapGen, true, swapsPerIteration, 2);

                for(; !astream.empty(); astream.nextBuffer())
                    swapAlgo.swap_buffer(astream.readBuffer());
                swapAlgo.flush();
            }

            {
                GlobalRewiringSwapGenerator rewiringSwapGenerator(_community_assignments, external_edges.size());
                rewiringSwapGenerator.pushEdges(stxxl::vector<edge_t>::bufreader_type(external_edges));
                rewiringSwapGenerator.generate();

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

                        swapAlgo.flush();

                        // FIXME do not push all edges but instead only push newly created edges.
                        rewiringSwapGenerator.pushEdges(stxxl::vector<edge_t>::bufreader_type(external_edges));
                        rewiringSwapGenerator.generate();
                    }
                }
            }

            swapAlgo.run();

            decltype(external_edges)::bufreader_type ext_edge_reader(external_edges);
            edge_t curEdge = {-1, -1};
            decltype(_edges)::bufwriter_type edge_writer(_edges);

            while (!ext_edge_reader.empty() || !edgeSorter.empty()) {
                if (ext_edge_reader.empty() || (!edgeSorter.empty() && *edgeSorter <= *ext_edge_reader)) {
                    // FIXME this simply discards duplicates, add rewiring!
                    if (curEdge != *edgeSorter) {
                        curEdge = *edgeSorter;
                        edge_writer << curEdge;
                    }

                    ++edgeSorter;
                } else if (edgeSorter.empty() || *ext_edge_reader < *edgeSorter) {
                    if (curEdge != *ext_edge_reader) {
                        curEdge = *ext_edge_reader;
                        edge_writer << curEdge;
                    } else {
                        assert(false && "Global edges should have been rewired to not to conflict with any internal edge!");
                    };

                    ++ext_edge_reader;
                }
            }
        }
    }
}
