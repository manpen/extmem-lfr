//
// Created by michael on 22.09.15.
//

#pragma once


#include <stxxl/vector>
#include <stx/btree_map>
#include "defs.h"

class EdgeVectorCache {
public:
    using vector_type = stxxl::VECTOR_GENERATOR<edge_t>::result;
    EdgeVectorCache(vector_type &external_edges);

    template <typename Iterator>
    inline void loadEdges(Iterator& edges);

    void flushEdges();

    template <typename Iterator>
    void loadAndFlushEdges(Iterator& edges);

    edge_t& getEdge(int_t id) {
        assert(_internal_edges.find(id) != _internal_edges.end());
        return _internal_edges[id];
    }

    void setEdge(int_t id, edge_t e) { _internal_edges[id] = e; };
private:
    vector_type &_external_edges;
    stx::btree_map<int_t, edge_t> _internal_edges;
};

template <typename Iterator>
void EdgeVectorCache::loadEdges(Iterator& edges) {
    // FIXME: which iterator type should be used? Is it already sorted?

    if (!_internal_edges.empty()) {
        throw std::runtime_error("Error, flush internal edges before loading new edges.");
    }

    int_t id = 0;

    vector_type::bufreader_type reader(_external_edges);
    while (!reader.empty()) {
        if (edges.empty()) break;

        if (*edges == id) {
            _internal_edges.insert(std::make_pair(id, *reader));
            ++edges;
        }

        ++reader;
        ++id;
    }
}
template <typename Iterator>
void EdgeVectorCache::loadAndFlushEdges(Iterator &edges) {
    flushEdges(); // FIXME do this in one EM scan
    loadEdges(edges);
}
