#include "LFR.h"

namespace LFR {
    void LFR::_merge_community_and_global_graph() {
        decltype(_inter_community_edges)::bufreader_type ext_edge_reader(_inter_community_edges);
        decltype(_intra_community_edges)::bufreader_type intra_edge_reader(_intra_community_edges);

        decltype(_edges)::bufwriter_type edge_writer(_edges);

        edge_t curEdge = {-1, -1};

        int_t discardedEdges = 0;

        while (!ext_edge_reader.empty() || !intra_edge_reader.empty()) {
            if (ext_edge_reader.empty() || (!intra_edge_reader.empty() && intra_edge_reader->edge <= *ext_edge_reader)) {
                if (curEdge != intra_edge_reader->edge) {
                    curEdge = intra_edge_reader->edge;
                    edge_writer << curEdge;
                } else {
                    ++discardedEdges;
                }

                ++intra_edge_reader;
            } else if (intra_edge_reader.empty() || *ext_edge_reader < intra_edge_reader->edge) {
                if (curEdge != *ext_edge_reader) {
                    curEdge = *ext_edge_reader;
                    edge_writer << curEdge;
                } else {
                    assert(false && "Global edges should have been rewired to not to conflict with any internal edge!");
                }

                ++ext_edge_reader;
            }
        }

        edge_writer.finish();

        if (discardedEdges > 0) {
            STXXL_MSG("Discarded " << discardedEdges << " internal edges that were in multiple communities of in total " << _edges.size() << " edges.");
        }
    }
}