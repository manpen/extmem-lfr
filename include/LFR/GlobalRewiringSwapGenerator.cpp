#include <LFR/GlobalRewiringSwapGenerator.h>
#include <stxxl/priority_queue>

GlobalRewiringSwapGenerator::GlobalRewiringSwapGenerator(const stxxl::vector< LFR::CommunityAssignment > &communityAssignment, edgeid_t numEdges)
    : _node_community_sorter(GenericComparatorStruct<NodeCommunity>::Ascending(), SORTER_MEM), _num_edges(numEdges) {

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

    using EdgeCommunityPQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<EdgeCommunity, EdgeCommunityPQComparator, PQ_INT_MEM, 1 << 20>::result;
    using EdgeCommunityPQBlock = typename EdgeCommunityPQ::block_type;

    // use pq in addition to _depchain_edge_sorter to pass messages between swaps
    stxxl::read_write_pool<EdgeCommunityPQBlock>
            pq_pool(PQ_POOL_MEM / 2 / EdgeCommunityPQBlock::raw_size,
                    PQ_POOL_MEM / 2 / EdgeCommunityPQBlock::raw_size);
    EdgeCommunityPQ edge_community_pq(pq_pool);


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
                edge_community_pq.push(EdgeCommunity {edgeReader->second, eid, com});
            }
            ++edgeReader;
            ++eid;
        }

        // read from PQ all edges from current node
        while (!edge_community_pq.empty() && edge_community_pq.top().head == u) {
            // check if any of their annotated communities are also in current edge, if yes: generate swap with random partner
            edgeid_t curEdge = edge_community_pq.top().eid;

            for (auto com : currentCommunities) {
                while (!edge_community_pq.empty() && edge_community_pq.top().eid == curEdge && edge_community_pq.top().tail_community < com) {
                    edge_community_pq.pop();
                }

                if (edge_community_pq.empty() || edge_community_pq.top().eid != curEdge) break;

                if (edge_community_pq.top().tail_community == com) {
                    // generate swap with random partner
                    SwapDescriptor swap = {edge_community_pq.top().eid, static_cast<edgeid_t>(_random_integer(_num_edges)), _random_flag(2)};
                    while (swap.edges()[0] == swap.edges()[1]) {
                        swap.edges()[1] = _random_integer(_num_edges);
                    }

                    result_writer << swap;

                    while (!edge_community_pq.empty() && edge_community_pq.top().eid == curEdge) {
                        edge_community_pq.pop();
                    }

                    break;
                }
            }
        }
    }

    _node_community_sorter.rewind();

    result_writer.finish();

    return result;
}

