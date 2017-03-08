#include "LFR.h"

namespace LFR {
    void LFR::_merge_community_and_global_graph() {
        _inter_community_edges.rewind();
        _intra_community_edges.rewind();

        _edges.clear();

        edge_t curEdge = {-1, -1};

        int_t discardedEdges = 0;

        while (!_inter_community_edges.empty() || !_intra_community_edges.empty()) {
            if (_inter_community_edges.empty() || (!_intra_community_edges.empty() && *_intra_community_edges <= *_inter_community_edges)) {
                if (curEdge != *_intra_community_edges) {
                    curEdge = *_intra_community_edges;
                    _edges.push(curEdge);
                } else {
                    ++discardedEdges;
                }

                ++_intra_community_edges;
            } else if (_intra_community_edges.empty() || *_inter_community_edges < *_intra_community_edges) {
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
            assert(false && "Duplicate intra-community edges should have been rewired!");
        }
    }
}
