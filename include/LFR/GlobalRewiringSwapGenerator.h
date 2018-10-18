#pragma once

#include <LFR/LFR.h>
#include <Swaps.h>
#include <GenericComparator.h>
#include <memory>
#include <stxxl/sequence>
#include <Utils/RandomBoolStream.h>
#include <random>

class GlobalRewiringSwapGenerator {
public:
    struct NodeCommunity {
        node_t node;
        community_t community;

        DECL_LEX_COMPARE(NodeCommunity, node, community)
    };

    struct EdgeCommunity {
        node_t head;
        node_t tail;
        community_t tail_community;

        DECL_LEX_COMPARE(EdgeCommunity, head, tail, tail_community)
    };

private:
    stxxl::sequence<NodeCommunity> _node_communities;
    using edge_community_sorter_t = stxxl::sorter<EdgeCommunity, GenericComparatorStruct<EdgeCommunity>::Ascending>;
    std::unique_ptr<stxxl::sequence<NodeCommunity>::stream> _node_community_reader; // when storing this by value, the end iterator is initialized too early...
    std::unique_ptr<edge_community_sorter_t> _edge_community_input_sorter;
    std::unique_ptr<edge_community_sorter_t> _edge_community_output_sorter;
    edgeid_t _num_edges;

    std::vector<community_t> _current_communities;
    node_t _current_node;
    SemiLoadedSwapDescriptor _swap;
    bool _empty;

    STDRandomEngine _rand_gen;
    std::uniform_int_distribution<edgeid_t> _rand_distr;
    RandomBoolStream _bool_stream;

public:
    GlobalRewiringSwapGenerator(const stxxl::vector<LFR::CommunityAssignment> &communityAssignment, edgeid_t numEdges, seed_t seed);

    /**
     * Add edges that shall be checked for conflicts by providing an STXXL stream interface to the edges.
     * The edges must be normalized and sorted in ascending order.
     * Edges can also be pushed when the stream of generated swaps is not empty.
     * Pushing edges does not affect the state of the swap stream.
     * New swaps will only be available after generate() has been called.
     *
     * @param edgeIterator the iterator of the edges.
     */
    template <typename Iterator>
    void pushEdges(Iterator &&edgeIterator) {
        if (_edge_community_input_sorter) {
            // we already have a sorter and this sorter should be initialized already.
            // this is either because it contains edges from a previous push or
            // because it was emptied before it was swapped from output to input
        } else if (_edge_community_output_sorter && empty()) {
            // the output was emptied already - we can re-use that sorter.
            // Note that on emptying, the sorter is cleared, so no clear necessaary here.
            // Note also that input does not contain any sorter, so this does not initialize the output sorter without sorting or discard any input.
            std::swap(_edge_community_input_sorter, _edge_community_output_sorter);
        } else {
            _edge_community_input_sorter.reset(new edge_community_sorter_t(edge_community_sorter_t::cmp_type(), SORTER_MEM));
        }

        decltype(_node_communities)::stream nodeCommunityReader(_node_communities);

        std::vector<community_t> currentCommunities;

        // for each node
        for (node_t u = 0; !nodeCommunityReader.empty(); ++u) {
            currentCommunities.clear();
            // read all communities from sorter
            while (!nodeCommunityReader.empty() && (*nodeCommunityReader).node == u) {
                currentCommunities.push_back((*nodeCommunityReader).community);
                ++nodeCommunityReader;
            }

            // iterate over edges in currentEdges that start with that node
            while (!edgeIterator.empty() && edgeIterator->first == u) {
                for (auto com : currentCommunities) {
                    // push reverse edge with community and edge id in PQ
                    _edge_community_input_sorter->push(EdgeCommunity {edgeIterator->second, edgeIterator->first, com});
                }
                ++edgeIterator;
            }
        }
    };

    /**
     * Start the generation process, finishes the push phase.
     */
    void generate();

    const SemiLoadedSwapDescriptor & operator * () const { return _swap; };
    const SemiLoadedSwapDescriptor * operator -> () const { return &_swap; };

    GlobalRewiringSwapGenerator & operator++();

    bool empty() const { return _empty; };

};
