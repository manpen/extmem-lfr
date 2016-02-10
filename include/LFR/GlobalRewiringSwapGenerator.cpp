#include <LFR/GlobalRewiringSwapGenerator.h>
#include <stxxl/priority_queue>

GlobalRewiringSwapGenerator::GlobalRewiringSwapGenerator(const stxxl::vector< LFR::CommunityAssignment > &communityAssignment, edgeid_t numEdges)
    : _node_community_sorter(GenericComparatorStruct<NodeCommunity>::Ascending(), SORTER_MEM), _edge_community_sorter(GenericComparatorStruct<EdgeCommunity>::Ascending(), SORTER_MEM), _num_edges(numEdges) {

    stxxl::vector<LFR::CommunityAssignment>::bufreader_type communityReader(communityAssignment);

    while (!communityReader.empty()) {
        _node_community_sorter.push(NodeCommunity {communityReader->node_id, communityReader->community_id});
        ++communityReader;
    }

    _node_community_sorter.sort();
}

stxxl::vector< SwapDescriptor > GlobalRewiringSwapGenerator::generate(const stxxl::vector< edge_t > &currentEdges) {
    stxxl::vector<SwapDescriptor> result;
    decltype(result)::bufwriter_type result_writer(result);
    stxxl::vector<edge_t>::bufreader_type edgeReader(currentEdges);
    edgeid_t eid = 0;

    std::vector<community_t> currentCommunities;

    // use pq in addition to _depchain_edge_sorter to pass messages between swaps

    // for each node
    for (node_t u = 0; !_node_community_sorter.empty(); ++u) {
        currentCommunities.clear();
        // read all communities from sorter
        while (!_node_community_sorter.empty() && _node_community_sorter->node == u) {
            currentCommunities.push_back(_node_community_sorter->community);
            ++_node_community_sorter;
        }

        // iterate over edges in currentEdges that start with that node
        while (!edgeReader.empty() && edgeReader->first == u) {
            for (auto com : currentCommunities) {
                // push reverse edge with community and edge id in PQ
                _edge_community_sorter.push(EdgeCommunity {edgeReader->second, eid, com});
            }
            ++edgeReader;
            ++eid;
        }

    }

    _edge_community_sorter.sort();
    _node_community_sorter.rewind();

    // for each node
    for (node_t u = 0; !_node_community_sorter.empty(); ++u) {
        currentCommunities.clear();
        // read all communities from sorter
        while (!_node_community_sorter.empty() && _node_community_sorter->node == u) {
            currentCommunities.push_back(_node_community_sorter->community);
            ++_node_community_sorter;
        }

        // read from PQ all edges from current node
        while (!_edge_community_sorter.empty() && _edge_community_sorter->head == u) {
            // check if any of their annotated communities are also in current edge, if yes: generate swap with random partner
            edgeid_t curEdge = _edge_community_sorter->eid;

            for (auto com : currentCommunities) {
                while (!_edge_community_sorter.empty() && _edge_community_sorter->eid == curEdge && _edge_community_sorter->tail_community < com) {
                    ++_edge_community_sorter;
                }

                if (_edge_community_sorter.empty() || _edge_community_sorter->eid != curEdge) break;

                if (_edge_community_sorter->tail_community == com) {
                    // generate swap with random partner
                    edgeid_t eid0 = _edge_community_sorter->eid;
                    edgeid_t eid1 = _random_integer(_num_edges);

                    while (eid0 == eid1) {
                        eid1 = _random_integer(_num_edges);
                    }

                    result_writer << SwapDescriptor {eid0, eid1, _random_flag(2)};

                    while (!_edge_community_sorter.empty() && _edge_community_sorter->eid == curEdge) {
                        ++_edge_community_sorter;
                    }

                    break;
                }
            }
        }
    }

    _node_community_sorter.rewind();
    _edge_community_sorter.clear();

    result_writer.finish();

    return result;
}

