#include "LFR.h"
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <SwapGenerator.h>
#include <stxxl/vector>
#include <stxxl/sorter>
#include <IMGraph.h>
#include <EdgeSwaps/IMEdgeSwap.h>
#include <Utils/AsyncStream.h>

#ifndef MAX_INTERNAL_EDGES
#define MAX_INTERNAL_EDGES (1 * IntScale::M)
#endif

#ifndef MAX_INTERNAL_NODES
#define MAX_INTERNAL_NODES (1 * IntScale::M)
#endif

namespace LFR {
    void LFR::_generate_community_graphs() {
        decltype(_community_assignments)::bufreader_type assignment_reader(_community_assignments);
        std::vector<node_t> node_ids;
        std::vector<degree_t> node_degrees;
        stxxl::vector<node_t> external_node_ids;
        stxxl::sorter<CommunityEdge, GenericComparatorStruct<CommunityEdge>::Ascending> edgeSorter(GenericComparatorStruct<CommunityEdge>::Ascending(), SORTER_MEM);
        stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> intra_edgeSorter(GenericComparator<edge_t>::Ascending(), SORTER_MEM);

        for (community_t i = 0; i < static_cast<community_t>(_community_cumulative_sizes.size()) - 1; ++i) {
            node_t com_size = _community_cumulative_sizes[i+1] - _community_cumulative_sizes[i];

            if (com_size < 2) {
                while (!assignment_reader.empty() && assignment_reader->community_id == i) {
                    ++assignment_reader;
                }
                continue; // no edges to create
            }

            int_t degree_sum = 0;

            HavelHakimiIMGenerator gen;
            bool internalNodes = (com_size < MAX_INTERNAL_NODES);

            if (internalNodes) {
                node_ids.clear();
                node_ids.reserve(com_size);
                node_degrees.clear();
                node_degrees.reserve(com_size);

                while (!assignment_reader.empty() && assignment_reader->community_id == i) {
                    node_degrees.push_back(assignment_reader->degree);
                    degree_sum += assignment_reader->degree;
                    node_ids.push_back(assignment_reader->node_id);
                    gen.push(assignment_reader->degree);
                    ++assignment_reader;
                }
            } else {
                stxxl::vector<node_t>::bufwriter_type node_id_writer(external_node_ids);

                while (!assignment_reader.empty() && assignment_reader->community_id == i) {
                    node_id_writer << assignment_reader->node_id;
                    degree_sum += assignment_reader->degree;
                    gen.push(assignment_reader->degree);
                    ++assignment_reader;
                }

                node_id_writer.finish();
            }

            gen.generate(HavelHakimiIMGenerator::DecreasingDegree);

            if (internalNodes && degree_sum < 2*MAX_INTERNAL_EDGES) {
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
                    edgeSorter.push(CommunityEdge(i, e));
                }
            } else {
                stxxl::vector<edge_t> intra_edges(degree_sum/2);

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

                int_t swapsPerIteration = intra_edges.size() / 3;

                // Generate swaps
                uint_t numSwaps = 10*intra_edges.size();
                SwapGenerator swapGen(numSwaps, intra_edges.size());

                // perform swaps
                EdgeSwapInternalSwaps swapAlgo(intra_edges, swapsPerIteration);

                AsyncStream<SwapGenerator> astream(swapGen, true, swapsPerIteration, 2);
                for(; !astream.empty(); astream.nextBuffer())
                    swapAlgo.swap_buffer(astream.readBuffer());

                swapAlgo.run();

                decltype(intra_edges)::bufreader_type edge_reader(intra_edges);

                if (internalNodes) {
                    while (!edge_reader.empty()) {
                        edge_t e = {node_ids[edge_reader->first], node_ids[edge_reader->second]};
                        e.normalize();
                        edgeSorter.push(CommunityEdge(i, e));
                        ++edge_reader;
                    }
                } else { // external memory mapping with an additional sort step
                    {
                        decltype(external_node_ids)::bufreader_type node_id_reader(external_node_ids);

                        for (node_t u = 0; !node_id_reader.empty(); ++u, ++node_id_reader) {
                            while (!edge_reader.empty() && edge_reader->first == u) {
                                intra_edgeSorter.push(edge_t {edge_reader->second, *node_id_reader});
                            }
                        }
                    }

                    intra_edgeSorter.sort();

                    {
                        decltype(external_node_ids)::bufreader_type node_id_reader(external_node_ids);

                        for (node_t u = 0; !node_id_reader.empty(); ++u, ++node_id_reader) {
                            while (!intra_edgeSorter.empty() && intra_edgeSorter->first == u) {
                                edge_t e(edge_reader->second, *node_id_reader);
                                e.normalize();
                                edgeSorter.push(CommunityEdge(i, e));
                            }
                        }
                    }

                    intra_edgeSorter.clear();
                }
            }
        }

        edgeSorter.sort();

        _intra_community_edges.reserve(edgeSorter.size());
        stxxl::stream::materialize(edgeSorter, _intra_community_edges.begin());
    }
}
