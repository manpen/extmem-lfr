//
// Created by michael on 22.09.15.
//

#pragma once


#include <stxxl/vector>
#include "defs.h"

class EdgeVector {
public:
    using vector_type = stxxl::VECTOR_GENERATOR<edge_t>::result;
    EdgeVector(vector_type &external_edges);

    template <typename Iterator>
    inline void loadEdges(Iterator edges);

    void flushEdges();

    template <typename Iterator>
    void loadAndFlushEdges(Iterator edges);

    edge_t getEdge(int_t id) { return _internal_edges[id]; }

    void setEdge(int_t id, node_t u, node_t v) { _internal_edges[id] = std::make_pair(u, v); };
private:
    vector_type &_external_edges;
    std::map<int_t, edge_t> _internal_edges;
};

template <typename Iterator>
void EdgeVector::loadEdges(Iterator edges) {
    // FIXME: which iterator type should be used? Is it already sorted?

    if (!_internal_edges.empty()) {
        throw std::runtime_error("Error, flush internal edges before loading new edges.");
    }

    int_t id = 0;
    for (auto it : vector_type::bufreader_type(_external_edges)) {
        if (edges.empty()) break;

        while (*edges == id) {
            _internal_edges[id] = it;
            ++edges;
        }

        ++id;
    }
}
template <typename Iterator>
void EdgeVector::loadAndFlushEdges(Iterator edges) {
    flushEdges(); // FIXME do this in one EM scan
    loadEdges(edges);
}
