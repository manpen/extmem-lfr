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
        edgeid_t eid;
        community_t tail_community;

        DECL_LEX_COMPARE(EdgeCommunity, head, eid, tail_community)
    };

private:
    stxxl::sorter<NodeCommunity, GenericComparatorStruct<NodeCommunity>::Ascending> _node_community_sorter;
    stxxl::sorter<EdgeCommunity, GenericComparatorStruct<EdgeCommunity>::Ascending> _edge_community_sorter;
    edgeid_t _num_edges;
    stxxl::random_number32 _random_flag;
    stxxl::random_number64 _random_integer;
public:
    GlobalRewiringSwapGenerator(const stxxl::vector<LFR::CommunityAssignment> &communityAssignment, edgeid_t numEdges);

    stxxl::vector<SwapDescriptor> generate(const stxxl::vector<edge_t> &currentEdges) ;
};
