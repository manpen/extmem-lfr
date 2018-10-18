#include <LFR/GlobalRewiringSwapGenerator.h>
#include <stxxl/priority_queue>

GlobalRewiringSwapGenerator::GlobalRewiringSwapGenerator(const stxxl::vector< LFR::CommunityAssignment > &communityAssignment, edgeid_t numEdges, seed_t seed)
    : _edge_community_input_sorter(new edge_community_sorter_t(GenericComparatorStruct<EdgeCommunity>::Ascending(), SORTER_MEM)),
      _num_edges(numEdges),
      _empty(true),
      _rand_gen(seed),
      _rand_distr(0, numEdges-1),
      _bool_stream(_rand_gen())
    {

    stxxl::sorter<NodeCommunity, GenericComparatorStruct<NodeCommunity>::Ascending> node_community_sorter(GenericComparatorStruct<NodeCommunity>::Ascending(), SORTER_MEM);
    #pragma omp critical (_community_assignment)
    {
        stxxl::vector<LFR::CommunityAssignment>::bufreader_type communityReader(communityAssignment);

        while (!communityReader.empty()) {
            node_community_sorter.push(NodeCommunity {communityReader->node_id, communityReader->community_id});
            ++communityReader;
        }
    }

    for (node_community_sorter.sort(); !node_community_sorter.empty(); ++node_community_sorter) {
        _node_communities.push_back(*node_community_sorter);
    }
}

void GlobalRewiringSwapGenerator::generate() {
    assert(empty());
    std::swap(_edge_community_input_sorter, _edge_community_output_sorter);
    _edge_community_output_sorter->sort();
    _node_community_reader.reset(new decltype(_node_communities)::stream(_node_communities));
    _current_node = 0;
    _empty = false;

    operator++();
}

GlobalRewiringSwapGenerator &GlobalRewiringSwapGenerator::operator++() {
    assert(_node_community_reader);
    while (!_edge_community_output_sorter->empty()) {
        while (!_node_community_reader->empty() && (**_node_community_reader).node == _current_node) {
            _current_communities.push_back((**_node_community_reader).community);
            ++(*_node_community_reader);
        }

        assert(!_current_communities.empty());

        // read from sorter all edges from current node
        while (!_edge_community_output_sorter->empty() && (*_edge_community_output_sorter)->head == _current_node) {
            // check if any of their annotated communities are also in current edge, if yes: generate swap with random partner
            const node_t curTail = (*_edge_community_output_sorter)->tail;
            const node_t curHead = (*_edge_community_output_sorter)->head;

            auto edgeComIsCurrentEdge = [&]() {
                return ((*_edge_community_output_sorter)->tail == curTail && (*_edge_community_output_sorter)->head == curHead);
            };

            for (auto com : _current_communities) {
                while (!(*_edge_community_output_sorter).empty() && edgeComIsCurrentEdge() && (*_edge_community_output_sorter)->tail_community < com) {
                    ++(*_edge_community_output_sorter);
                }

                if ((*_edge_community_output_sorter).empty() || !edgeComIsCurrentEdge()) break;

                if ((*_edge_community_output_sorter)->tail_community == com) {
                    // generate swap with random partner
                    edgeid_t eid1 = _rand_distr(_rand_gen);

                    _swap = SemiLoadedSwapDescriptor {edge_t {(*_edge_community_output_sorter)->tail, (*_edge_community_output_sorter)->head}, eid1, *_bool_stream};
                    ++_bool_stream;

                    // forward till the end of the current edge such that the next swap will be for another edge
                    while (!(*_edge_community_output_sorter).empty() && edgeComIsCurrentEdge()) {
                        ++(*_edge_community_output_sorter);
                    }

                    return *this;
                }
            }

            // FIXME: in the first version I didn't have this loop.
            // But if there are entries for the current edge with tail_community larger than the largest community in _current_communities this should be necessary.
            while (!(*_edge_community_output_sorter).empty() && edgeComIsCurrentEdge()) {
                ++(*_edge_community_output_sorter);
            }
        }

        ++_current_node;
        _current_communities.clear();
    }

    _node_community_reader.reset(nullptr);
    _edge_community_output_sorter->clear();

    _empty = true;

    return *this;
}
