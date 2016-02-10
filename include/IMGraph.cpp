#include <IMGraph.h>
#include <tuple>
#include <cassert>

IMGraph::IMGraph(const std::vector<degree_t> &degreeSequence) : _first_head(degreeSequence.size() + 1), _last_head(degreeSequence.size() + 1) {
    int_t sum = 0;
    for (size_t i = 0; i < degreeSequence.size(); ++i) {
        _first_head[i] = sum;
        _last_head[i] = sum;
        sum += degreeSequence[i];
    }
    _first_head[degreeSequence.size()] = sum;
    _last_head[degreeSequence.size()] = sum;
    _head.resize(sum);
};

SwapResult IMGraph::swapEdges(const edgeid_t eid0, const edgeid_t eid1, bool direction) {
    auto &idx0 = _edge_index[eid0];
    auto &idx1 = _edge_index[eid1];
    SwapResult result;

    edge_t e[2] = {{_head[idx0.second], _head[idx0.first]}, {_head[idx1.second], _head[idx1.first]}};
    edge_t t[2];
    std::tie(t[0], t[1]) = _swap_edges(e[0], e[1], direction);

    result.edges[0] = t[0];
    result.edges[1] = t[1];

    // check for conflict: loop
    if (t[0].first == t[0].second || t[1].first == t[1].second) {
        result.loop = true;
    } else { // check for conflict edges
        result.loop = false;
        result.conflictDetected[0] = false;
        result.conflictDetected[1] = false;
        for (unsigned char pos = 0; pos < 2; ++pos) {
            node_t src = t[pos].first;
            for (edgeid_t i = _first_head[src]; i < _first_head[src+1]; ++i) {
                if (_head[i] == t[pos].second) {
                    result.conflictDetected[pos] = true;
                }
            }
        }
    }

    result.performed = !result.loop && !(result.conflictDetected[0] || result.conflictDetected[1]);

    if (result.performed) {
        if (!direction) { // reverse first edge
            std::swap(idx0.first, idx0.second);
        }

        // cross actual nodes of edges
        std::swap(_head[idx0.first], _head[idx1.first]);
        std::swap(_head[idx0.second], _head[idx1.second]);

        // correct the index to let each one point to head and tail of a single edge
        std::swap(idx0.first, idx1.first);

        // normalize direction of edges in the index
        if (_head[idx0.first] < _head[idx0.second]) {
            std::swap(idx0.first, idx0.second);
        }

        if (_head[idx1.first] < _head[idx1.second]) {
            std::swap(idx1.first, idx1.second);
        }

        // make sure we did the same as the edge swap implementation
        assert((t[0] == edge_t {_head[idx0.second], _head[idx0.first]} && t[1] == edge_t {_head[idx1.second], _head[idx1.first]}));
    }

    result.normalize();

    return result;
}

IMGraph::IMEdgeStream IMGraph::getEdges() const {
    return IMEdgeStream(*this);
}



