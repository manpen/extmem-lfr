#include <utility>

#include "LFR.h"
#include "GlobalRewiringSwapGenerator.h"
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeSwaps/SemiLoadedEdgeSwapTFP.h>
#include <Utils/AsyncStream.h>
#include <Utils/IOStatistics.h>
#include <SwapGenerator.h>
#include <Utils/NodeHash.h>
#include <Curveball/EMCurveball.h>
#include <Utils/RandomSeed.h>

namespace LFR {
    void LFR::_generate_global_graph(int_t globalSwapsPerIteration) {
        #ifdef CURVEBALL_RAND
        HavelHakimiIMGeneratorWithDeficits gen(HavelHakimiIMGeneratorWithDeficits::DecreasingDegree);
        DegreeStream temp_rewindable_ext_degrees;
        class FixedDegreeStreamWrapper {
        protected:
            DegreeStream & _unrealisable_degrees;
            std::vector<std::pair<node_t, degree_t>> & _deficits;
            std::vector<std::pair<node_t, degree_t>>::const_iterator _deficits_it;
            node_t _count = 0;


        public:
            FixedDegreeStreamWrapper(DegreeStream & unrealisable_degrees,
                                     std::vector<std::pair<node_t, degree_t>> & deficits,
                                     node_t count = 0)
                    : _unrealisable_degrees(unrealisable_degrees),
                      _deficits(deficits),
                      _deficits_it(deficits.cbegin()),
                      _count(count)
            { }

            void rewind() {
                _unrealisable_degrees.rewind();
                _count = 0;
                _deficits_it = _deficits.cbegin();
            }

            bool empty() const {
                return _unrealisable_degrees.empty();
            }

            FixedDegreeStreamWrapper & operator ++() {
                ++_count;
                ++_unrealisable_degrees;
                return *this;
            }

            degree_t operator * () {
                if (LIKELY(_deficits_it == _deficits.cend() || _count != (*_deficits_it).first))
                    return *_unrealisable_degrees;
                else {
                    const degree_t unrealised_degree = (*_unrealisable_degrees);
                    const degree_t deficit = (*_deficits_it).second;
                    ++_deficits_it;

                    return (unrealised_degree - deficit);
                }
            }
        };
        #else
        HavelHakimiIMGenerator gen(HavelHakimiIMGenerator::DecreasingDegree);
        #endif
        {
            using deg_node_t = std::pair<degree_t, node_t>;
            stxxl::sorter<deg_node_t, GenericComparator<deg_node_t>::Descending> extDegree(GenericComparator<deg_node_t>::Descending(), SORTER_MEM);

            int_t degree_sum = 0;

            { // push node degrees in descending order in generator
                _node_sorter.rewind();

                for(node_t nid = 0; !_node_sorter.empty(); ++_node_sorter, ++nid) {
                    extDegree.push({_node_sorter->externalDegree(_mixing), nid});
                    #ifdef CURVEBALL_RAND
                    temp_rewindable_ext_degrees.push(_node_sorter->externalDegree(_mixing));
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
                    const edge_t & orig_edge = *gen;

                    // this for loop only does something if the current run of nodes,
                    // which was i to this point is no longer i, when that run ends,
                    // we have to forward the external degree stream until we hit the
                    // next node of the next run
                    for (; i < orig_edge.first; ++extDegree, ++i);

                    // the first entry of the generated Havel-Hakimi edge not necessarily
                    // matches the second entry of the current external degree stream, since
                    // it was sorted previously and just mapped 1-1 starting from 0,
                    // additionally it may not have the same degree, in a Havel-Hakimi
                    // materialisation unsatisfied nodes and edges can occur
                    edge_sorter1.push({orig_edge.second, (*extDegree).second});
                }
                assert(gen.unsatisfiedNodes() == static_cast<node_t>(gen.get_deficits().size()));

                edge_sorter1.sort();
                extDegree.rewind();

                #ifdef CURVEBALL_RAND
                bool check_deficits = (gen.unsatisfiedNodes() > 0);
                auto & deficits = gen.get_deficits();
                auto deficits_it = deficits.begin();
                for (node_t i = 0; !edge_sorter1.empty(); ++edge_sorter1) {
                    const edge_t &orig_edge = *edge_sorter1;
                    for (; i < orig_edge.first; ++extDegree, ++i);

                    if (check_deficits) {
                        auto & node_deficit_pair = *deficits_it;
                        if (UNLIKELY(i == node_deficit_pair.first)) {
                            node_deficit_pair.first = (*extDegree).second;
                            assert(node_deficit_pair.first == (*extDegree).second);

                            ++deficits_it;
                            if (deficits_it == deficits.end())
                                check_deficits = false;
                        }
                    }

                    assert(orig_edge.second != (*extDegree).second);
                    edge_sorter2.push({orig_edge.second, (*extDegree).second});
                }
                #else
                for (node_t i = 0; !edge_sorter1.empty(); ++edge_sorter1) {
                    const edge_t &orig_edge = *edge_sorter1;
                    for (; i < orig_edge.first; ++extDegree, ++i);

                    assert(orig_edge.second != (*extDegree).second);
                    edge_sorter2.push({orig_edge.second, (*extDegree).second});
                }
                #endif
            }

            edge_sorter2.sort();

            _inter_community_edges.clear();
            StreamPusher<decltype(edge_sorter2), decltype(_inter_community_edges)> (edge_sorter2, _inter_community_edges);
            _inter_community_edges.consume();
        }

        std::cout << "Current EM allocation after InitialGlobalGen: " <<  stxxl::block_manager::get_instance()->get_current_allocation() << std::endl;
        std::cout << "Maximum EM allocation after InitialGlobalGen: " <<  stxxl::block_manager::get_instance()->get_maximum_allocation() << std::endl;

        {
            #ifndef NDEBUG
            const size_t num_inter_community_edges = _inter_community_edges.size();
            #endif

            #ifdef CURVEBALL_RAND
            temp_rewindable_ext_degrees.rewind();

            if (1) {
                IOStatistics ios("GlobalGenInitialRandCurveball");
                if (gen.unsatisfiedNodes() == 0) {
                    using CurveballType = Curveball::EMCurveball<Curveball::ModHash>;
                    CurveballType randAlgo(_inter_community_edges,
                                           temp_rewindable_ext_degrees,
                                           _number_of_nodes,
                                           20,
                                           _inter_community_edges,
                                           omp_get_max_threads(),
                                           _max_memory_usage,
                                           true); // change to false when it is possible
                    randAlgo.run();
                } else if (gen.unsatisfiedNodes() > 0){
                    FixedDegreeStreamWrapper rewindable_ext_degrees(temp_rewindable_ext_degrees, gen.get_deficits());

                    using CurveballType = Curveball::EMCurveball<Curveball::ModHash, FixedDegreeStreamWrapper>;
                    CurveballType randAlgo(_inter_community_edges,
                                           rewindable_ext_degrees,
                                           _number_of_nodes,
                                           20,
                                           _inter_community_edges,
                                           omp_get_max_threads(),
                                           _max_memory_usage,
                                           true); // change to false when it is possible
                    randAlgo.run();
                }
            }

            _inter_community_edges.rewind();

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

            #ifndef NDEBUG
            assert(_inter_community_edges.size() == num_inter_community_edges);
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
