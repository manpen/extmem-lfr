#include <IMGraph.h>
#include <tuple>
#include <cassert>

IMGraph::IMGraph(const std::vector<degree_t> &degreeSequence) {
#ifndef NDEBUG
    if (!std::is_sorted(degreeSequence.begin(), degreeSequence.end(), std::greater<degree_t>())) {
        STXXL_MSG("WARNING: degree sequence is not sorted in descending order, performance of IMGraph will be degraded!");
    }
#endif

    // we store the first H (inspired by the H-index, but a bit different) edges in an adjacency matrix
    // we choose H such that the entries in the adjacency array would need as much memory as the adjacency matrix needs
    // note that each entry in the adjacency array needs 64 bits while an entry in the adjacency matrix needs only 1 bit
    int_t sum = 0;
    _h = degreeSequence.size();
    bool has_h = false;
    for (size_t i = 0; i < degreeSequence.size(); ++i) {
        // TODO adjust factor if less memory shall be consumed
        if (!has_h && (sum + degreeSequence[i]) * 32 < static_cast<node_t>((i+1)*(i+1))) {
            has_h = true;
            _h = i;
            _first_head.reserve(degreeSequence.size() + 1 - _h);
            _last_head.reserve(degreeSequence.size() + 1 - _h);
            sum = 0;
        }

        if (has_h) {
            _first_head.push_back(sum);
            _last_head.push_back(sum);
        }
        sum += degreeSequence[i];
    }

    if (UNLIKELY(sum >= IMGraph::maxEdges())) {
        throw std::runtime_error("Error, too many edges for internal graph. The internal graph supports at maximum 2 billion edges");
    }

    STXXL_MSG("Putting first " << _h << " of in total " << degreeSequence.size() << " nodes in adjacency matrix");
    _first_head.push_back(sum);
    _last_head.push_back(sum);
    _head.resize(sum);
    assert(_first_head.size() == degreeSequence.size() + 1 - _h);
    assert(_last_head.size() == degreeSequence.size() + 1 - _h);
    _adjacency_matrix.resize(_h * _h);
};

SwapResult IMGraph::swapEdges(const edgeid_t eid0, const edgeid_t eid1, bool direction) {
    SwapResult result;

    edge_t e[2] = {getEdge(eid0), getEdge(eid1)};
    edge_t t[2];
    std::tie(t[0], t[1]) = _swap_edges(e[0], e[1], direction);

    result.edges[0] = t[0];
    result.edges[1] = t[1];

    // check for conflict: loop
    if (t[0].first == t[0].second || t[1].first == t[1].second) {
        result.loop = true;
    } else { // check for conflict edges
        result.loop = false;
        for (unsigned char pos = 0; pos < 2; ++pos) {
            result.conflictDetected[pos] = hasEdge(t[pos].first, t[pos].second);
        }
    }

    result.performed = !result.loop && !(result.conflictDetected[0] || result.conflictDetected[1]);

    if (result.performed) {
        auto &idx0 = _edge_index[eid0];
        auto &idx1 = _edge_index[eid1];

        // reset target as we need the non-normalized target edges
        t[0] = e[0];
        t[1] = e[1];

        if (!direction) { // reverse first edge
            std::swap(idx0.first, idx0.second);
            std::swap(t[0].first, t[0].second);
        }

        // execute swap
        std::swap(idx0.second, idx1.second);
        std::swap(t[0].first, t[1].first);

        // update adjacency matrix
        for (unsigned char pos = 0; pos < 2; ++pos) {
            if (e[pos].first < _h && e[pos].second < _h) {
                _adjacency_matrix[e[pos].first * _h + e[pos].second] = false;
                _adjacency_matrix[e[pos].second * _h + e[pos].first] = false;
            }
            if (t[pos].first < _h && t[pos].second < _h) {
                _adjacency_matrix[t[pos].first * _h + t[pos].second] = true;
                _adjacency_matrix[t[pos].second * _h + t[pos].first] = true;
            }
        }

        // update adjacency array or index
        if (!idx0.first.index_is_node) {
            _head[idx0.first.index] = t[0].first;
        } else {
            idx0.first.index = t[0].first;
        }

        if (!idx0.second.index_is_node) {
            _head[idx0.second.index] = t[0].second;
        } else {
            idx0.second.index = t[0].second;
        }

        if (!idx1.first.index_is_node) {
            _head[idx1.first.index] = t[1].first;
        } else {
            idx1.first.index = t[1].first;
        }

        if (!idx1.second.index_is_node) {
            _head[idx1.second.index] = t[1].second;
        } else {
            idx1.second.index = t[1].second;
        }

        // normalize direction of edges in the index
        if (t[0].first > t[0].second) {
            std::swap(idx0.first, idx0.second);
#ifndef NDEBUG // we only need the normalized edge for the assertion below
            t[0].normalize();
#endif
        }

        if (t[1].first > t[1].second) {
            std::swap(idx1.first, idx1.second);
#ifndef NDEBUG
            t[1].normalize();
#endif
        }

        // make sure the edge is correctly stored
        assert(t[0] == getEdge(eid0) && t[1] == getEdge(eid1));
        assert(hasEdge(t[0].first, t[0].second) && hasEdge(t[1].first, t[1].second));
        // make sure our swap implementation is the same as _swap_edges
        assert(t[0] == result.edges[0] && t[1] == result.edges[1]);
    }

    result.normalize();

    return result;
}

IMGraph::IMEdgeStream IMGraph::getEdges() const {
    return IMEdgeStream(*this);
}



