#include "LFR.h"

namespace LFR {
    void LFR::_merge_community_and_global_graph() {
        _inter_community_edges.rewind();
        decltype(_intra_community_edges)::bufreader_type intra_edge_reader(_intra_community_edges);

        _edges.clear();

        edge_t curEdge = {-1, -1};

        int_t discardedEdges = 0;

        while (!_inter_community_edges.empty() || !intra_edge_reader.empty()) {
            if (_inter_community_edges.empty() || (!intra_edge_reader.empty() && intra_edge_reader->edge <= *_inter_community_edges)) {
                if (curEdge != intra_edge_reader->edge) {
                    curEdge = intra_edge_reader->edge;
                    _edges.push(curEdge);
                } else {
                    ++discardedEdges;
                }

                ++intra_edge_reader;
            } else if (intra_edge_reader.empty() || *_inter_community_edges < intra_edge_reader->edge) {
                if (curEdge != *_inter_community_edges) {
                    curEdge = *_inter_community_edges;
                    _edges.push(curEdge);
                } else {
                    assert(false && "Global edges should have been rewired to not to conflict with any internal edge!");
                }

                ++_inter_community_edges;
            }
        }

        if (discardedEdges > 0) {
            STXXL_MSG("Discarded " << discardedEdges << " internal edges that were in multiple communities of in total " << _edges.size() << " edges.");
        }
    }
}
