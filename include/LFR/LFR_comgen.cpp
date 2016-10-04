#include "LFR.h"
#include "CommunityEdgeRewiringSwaps.h"
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <SwapGenerator.h>
#include <stxxl/vector>
#include <stxxl/sorter>
#include <IMGraph.h>
#include <EdgeSwaps/IMEdgeSwap.h>
#include <Utils/AsyncStream.h>
#include <omp.h>
#include <EdgeSwaps/EdgeSwapTFP.h>
#include <Utils/StreamPusher.h>

namespace LFR {
    void LFR::_generate_community_graphs() {
        stxxl::sorter<CommunityEdge, GenericComparatorStruct<CommunityEdge>::Ascending> edgeSorter(GenericComparatorStruct<CommunityEdge>::Ascending(), SORTER_MEM);
        const uint_t n_threads = omp_get_max_threads();
        const uint_t memory_per_thread = _max_memory_usage / n_threads;

        #pragma omp parallel shared(edgeSorter), num_threads(n_threads)
        {
            // set-up thread-private variables
            stxxl::vector<node_t> external_node_ids;

            #pragma omp for schedule(dynamic, 1)
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
                        assert(node_ids.empty() || node_ids.back() != ca.node_id);
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

                if (internalNodes && IMGraph::memoryUsage(com_size, degree_sum/2) < available_memory && degree_sum/2 < IMGraph::maxEdges()) {
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
                        for (SwapGenerator swapGen(numSwaps, graph.numEdges(), _seed_seq()); !swapGen.empty(); ++swapGen) {
                            swapAlgo.push(*swapGen);
                        }

                        swapAlgo.run();
                    }

#ifndef NDEBUG
                    edge_t last_e(edge_t::invalid());
#endif

                    #pragma omp critical (_edgeSorter)
                    for (auto it = graph.getEdges(); !it.empty(); ++it) {
                        edge_t e = {node_ids[it->first], node_ids[it->second]};
                        e.normalize();
#ifndef NDEBUG
                                    assert(e != last_e);
                                    assert(!e.is_loop());
                                    last_e = e;
#endif
                        edgeSorter.push(CommunityEdge(com, e));
                    }
                } else {
                    EdgeStream intra_edges;

                    for (; !gen.empty(); ++gen) {
                        assert(gen->first < gen->second);
                        intra_edges.push(*gen);
                    }

                    // Generate swaps
                    uint_t numSwaps = 10*intra_edges.size();
                    SwapGenerator swap_gen(numSwaps, intra_edges.size(), _seed_seq());

                    uint_t run_length = intra_edges.size() / 8;

                    // perform swaps
                    EdgeSwapTFP::EdgeSwapTFP swap_algo(intra_edges, run_length, _number_of_nodes, _max_memory_usage);

                    StreamPusher<decltype(swap_gen), decltype(swap_algo)>(swap_gen, swap_algo);

                    swap_algo.run();

                    intra_edges.rewind();

                    if (internalNodes) {
                        #pragma omp critical (_edgeSorter)
                        while (!intra_edges.empty()) {
                            edge_t e = {node_ids[intra_edges->first], node_ids[intra_edges->second]};
                            e.normalize();
                            edgeSorter.push(CommunityEdge(com, e));
                            ++intra_edges;
                        }
                    } else { // external memory mapping with an additional sort step
                        stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> intra_edgeSorter(GenericComparator<edge_t>::Ascending(), SORTER_MEM);

                        {
                            decltype(external_node_ids)::bufreader_type node_id_reader(external_node_ids);

                            for (node_t u = 0; !node_id_reader.empty(); ++u, ++node_id_reader) {
                                while (!intra_edges.empty() && intra_edges->first == u) {
                                    intra_edgeSorter.push(edge_t {intra_edges->second, *node_id_reader});
                                    ++intra_edges;
                                }
                            }
                        }

                        intra_edgeSorter.sort();

                        {
                            decltype(external_node_ids)::bufreader_type node_id_reader(external_node_ids);

#ifndef NDEBUG
                            edge_t last_e(edge_t::invalid());
#endif
                            #pragma omp critical (_edgeSorter)
                            for (node_t u = 0; !node_id_reader.empty(); ++u, ++node_id_reader) {
                                while (!intra_edgeSorter.empty() && intra_edgeSorter->first == u) {
                                    edge_t e(intra_edgeSorter->second, *node_id_reader);
                                    e.normalize();
#ifndef NDEBUG
                                    assert(e != last_e);
                                    assert(!e.is_loop());
                                    last_e = e;
#endif
                                    edgeSorter.push(CommunityEdge(com, e));
                                    ++intra_edgeSorter;
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

        CommunityEdgeRewiringSwaps rewiringSwaps(_intra_community_edges, _intra_community_edges.size() / 3);
        rewiringSwaps.run();
    }
}
