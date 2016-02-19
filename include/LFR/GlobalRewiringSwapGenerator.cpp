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

void GlobalRewiringSwapGenerator::generate() {
    _edge_community_sorter.sort();
    _node_community_sorter.rewind();
    _current_node = 0;
    _empty = false;

    operator++();
}

GlobalRewiringSwapGenerator &GlobalRewiringSwapGenerator::operator++() {
    while (!_edge_community_sorter.empty()) {
        while (!_node_community_sorter.empty() && _node_community_sorter->node == _current_node) {
            _current_communities.push_back(_node_community_sorter->community);
            ++_node_community_sorter;
        }

        // read from sorter all edges from current node
        while (!_edge_community_sorter.empty() && _edge_community_sorter->head == _current_node) {
            // check if any of their annotated communities are also in current edge, if yes: generate swap with random partner
            const node_t curTail = _edge_community_sorter->tail;
            const node_t curHead = _edge_community_sorter->head;

            auto edgeComIsCurrentEdge = [&]() {
                return (_edge_community_sorter->tail == curTail && _edge_community_sorter->head == curHead);
            };

            for (auto com : _current_communities) {
                while (!_edge_community_sorter.empty() && edgeComIsCurrentEdge() && _edge_community_sorter->tail_community < com) {
                    ++_edge_community_sorter;
                }

                if (_edge_community_sorter.empty() || !edgeComIsCurrentEdge()) break;

                if (_edge_community_sorter->tail_community == com) {
                    // generate swap with random partner
                    edgeid_t eid1 = _random_integer(_num_edges);

                    _swap = SemiLoadedSwapDescriptor {edge_t {_edge_community_sorter->tail, _edge_community_sorter->head}, eid1, _random_flag(2)};

                    while (!_edge_community_sorter.empty() && edgeComIsCurrentEdge()) {
                        ++_edge_community_sorter;
                    }

                    return *this;
                }
            }
        }

        ++_current_node;
        _current_communities.clear();
    }

    _empty = true;

    return *this;
}
