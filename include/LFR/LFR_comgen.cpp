#include "LFR.h"
#include "CommunityEdgeRewiringSwaps.h"
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <SwapGenerator.h>
#include <stxxl/vector>
#include <stxxl/sorter>
#include <IMGraph.h>
#include <EdgeSwaps/IMEdgeSwap.h>
#include <Utils/AsyncStream.h>
#include <omp.h>

namespace LFR {
    void LFR::_generate_community_graphs() {
        stxxl::sorter<CommunityEdge, GenericComparatorStruct<CommunityEdge>::Ascending> edgeSorter(GenericComparatorStruct<CommunityEdge>::Ascending(), SORTER_MEM);
        const uint_t n_threads = std::max(1, omp_get_max_threads() - 1);
        const uint_t memory_per_thread = _max_memory_usage / n_threads;

        //#pragma omp parallel shared(edgeSorter), num_threads(n_threads)
        {
            // set-up thread-private variables
            stxxl::vector<node_t> external_node_ids;

            //#pragma omp for schedule(dynamic, 1)
            for (community_t com = 0; com < static_cast<community_t>(_community_cumulative_sizes.size()) - 1; ++com) {
                node_t com_size = _community_cumulative_sizes[com+1] - _community_cumulative_sizes[com];
                std::vector<node_t> node_ids;
                std::vector<degree_t> node_degrees;

                if (com_size < 2) {
                    continue; // no edges to create
                }

                int_t degree_sum = 0;
                uint_t available_memory = memory_per_thread;

                HavelHakimiIMGenerator gen(HavelHakimiIMGenerator::DecreasingDegree);
                bool internalNodes = (com_size * 2 * sizeof(node_t) < available_memory / 10 ); // use up to ten percent of the memory for internal node ids

                if (internalNodes) {
                    available_memory -= (com_size * 2 * sizeof(node_t));
                    node_ids.reserve(com_size);
                    node_degrees.reserve(com_size);

                    #pragma omp critical (_community_assignment)
                    for (auto it(_community_assignments.cbegin() + _community_cumulative_sizes[com]); it < _community_assignments.cbegin() + _community_cumulative_sizes[com+1]; ++it) {
                        const auto ca = *it;
                        assert(ca.community_id == com);
                        node_degrees.push_back(ca.degree);
                        degree_sum += ca.degree;
                        node_ids.push_back(ca.node_id);
                        gen.push(ca.degree);
                    }
                } else {
                    external_node_ids.clear();
                    external_node_ids.resize(com_size);
                    stxxl::vector<node_t>::bufwriter_type node_id_writer(external_node_ids);

                    #pragma omp critical (_community_assignment)
                    for (auto it(_community_assignments.cbegin() + _community_cumulative_sizes[com]); it < _community_assignments.cbegin() + _community_cumulative_sizes[com+1]; ++it) {
                        const auto ca = *it;
                        assert(ca.community_id == com);
                        node_id_writer << ca.node_id;
                        degree_sum += ca.degree;
                        gen.push(ca.degree);
                    }

                    node_id_writer.finish();
                }

                gen.generate();

                if (internalNodes && ((sizeof(node_t) * 4 + 2) * degree_sum/2 + sizeof(edgeid_t) * com_size) < available_memory) {
                    IMGraph graph(node_degrees);
                    while (!gen.empty()) {
                        graph.addEdge(*gen);
                        ++gen;
                    }

                    STXXL_MSG("Running internal swaps with " << graph.numEdges() << " edges");

                    if (graph.numEdges() > 1) {
                        // Generate swaps
                        uint_t numSwaps = 10*graph.numEdges();

                        IMEdgeSwap swapAlgo(graph);
                        for (SwapGenerator swapGen(numSwaps, graph.numEdges()); !swapGen.empty(); ++swapGen) {
                            swapAlgo.push(*swapGen);
                        }

                        swapAlgo.run();
                    }

                    #pragma omp critical (_edgeSorter)
                    for (auto it = graph.getEdges(); !it.empty(); ++it) {
                        edge_t e = {node_ids[it->first], node_ids[it->second]};
                        e.normalize();
                        edgeSorter.push(CommunityEdge(com, e));
                    }
                } else {
                    stxxl::vector<edge_t> intra_edges(degree_sum/2);

                    available_memory -= SORTER_MEM; // internal swaps need a sorter! and we made sure there is memory for one.

                    // FIXME check if this can be eliminated, i.e. if output is already sorted!
                    {
                        stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> intra_edgeSorter(GenericComparator<edge_t>::Ascending(), SORTER_MEM);

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
                        intra_edges.resize(endIt - intra_edges.begin());
                    }

                    int_t swapsPerIteration = std::min<int_t>(intra_edges.size() / 4, (available_memory)/100);
                    if (swapsPerIteration*100 < static_cast<int_t>(intra_edges.size())) {
                        STXXL_ERRMSG("With the given memory, more than 1000 swap iterations are required! Consider increasing the amount of available memory.");
                    }

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
                        #pragma omp critical (_edgeSorter)
                        while (!edge_reader.empty()) {
                            edge_t e = {node_ids[edge_reader->first], node_ids[edge_reader->second]};
                            e.normalize();
                            edgeSorter.push(CommunityEdge(com, e));
                            ++edge_reader;
                        }
                    } else { // external memory mapping with an additional sort step
                        stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> intra_edgeSorter(GenericComparator<edge_t>::Ascending(), SORTER_MEM);

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

                            #pragma omp critical (_edgeSorter)
                            for (node_t u = 0; !node_id_reader.empty(); ++u, ++node_id_reader) {
                                while (!intra_edgeSorter.empty() && intra_edgeSorter->first == u) {
                                    edge_t e(edge_reader->second, *node_id_reader);
                                    e.normalize();
                                    edgeSorter.push(CommunityEdge(com, e));
                                }
                            }
                        }

                    }
                }
            }
        }

        edgeSorter.sort();

        _intra_community_edges.resize(edgeSorter.size());
        stxxl::stream::materialize(edgeSorter, _intra_community_edges.begin());

        //CommunityEdgeRewiringSwaps rewiringSwaps(_intra_community_edges, _intra_community_edges.size() / 3);
        //rewiringSwaps.run();
    }
}
