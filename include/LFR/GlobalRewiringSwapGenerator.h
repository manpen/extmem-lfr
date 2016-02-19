#pragma once

#include <LFR/LFR.h>
#include <Swaps.h>
#include <GenericComparator.h>

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
    stxxl::sorter<NodeCommunity, GenericComparatorStruct<NodeCommunity>::Ascending> _node_community_sorter;
    stxxl::sorter<EdgeCommunity, GenericComparatorStruct<EdgeCommunity>::Ascending> _edge_community_sorter;
    edgeid_t _num_edges;
    stxxl::random_number32 _random_flag;
    stxxl::random_number64 _random_integer;
    std::vector<community_t> _current_communities;
    node_t _current_node;
    SemiLoadedSwapDescriptor _swap;
    bool _empty;
public:
    GlobalRewiringSwapGenerator(const stxxl::vector<LFR::CommunityAssignment> &communityAssignment, edgeid_t numEdges);

    /**
     * Add edges that shall be checked for conflicts by providing an STXXL stream interface to the edges.
     * The edges must be normalized and sorted in ascending order.
     * If not all swaps have been processed, the remaining problematic edges are merged with the given edges.
     *
     * @param edgeIterator the iterator of the edges.
     */
    template <typename Iterator>
    void pushEdges(Iterator &&edgeIterator) {
        // TODO: before using _edge_community_sorter, make sure it is empty by removing and then merging the remaining edges.
        _edge_community_sorter.clear();
        _node_community_sorter.rewind();

        std::vector<community_t> currentCommunities;

        // for each node
        for (node_t u = 0; !_node_community_sorter.empty(); ++u) {
            currentCommunities.clear();
            // read all communities from sorter
            while (!_node_community_sorter.empty() && _node_community_sorter->node == u) {
                currentCommunities.push_back(_node_community_sorter->community);
                ++_node_community_sorter;
            }

            // iterate over edges in currentEdges that start with that node
            while (!edgeIterator.empty() && edgeIterator->first == u) {
                for (auto com : currentCommunities) {
                    // push reverse edge with community and edge id in PQ
                    _edge_community_sorter.push(EdgeCommunity {edgeIterator->second, edgeIterator->first, com});
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
