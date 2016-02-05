#include <IMGraph.h>
#include <tuple>

IMGraph::IMGraph(const std::vector<degree_t> &degreeSequence) : _first_tail(degreeSequence.size() + 1), _last_tail(degreeSequence.size() + 1) {
    int_t sum = 0;
    for (size_t i = 0; i < degreeSequence.size(); ++i) {
        _first_tail[i] = sum;
        _last_tail[i] = sum;
        sum += degreeSequence[i];
    }
    _first_tail[degreeSequence.size()] = sum;
    _last_tail[degreeSequence.size()] = sum;
    _tail.resize(sum);
};

SwapResult IMGraph::swapEdges(const edgeid_t eid0, const edgeid_t eid1, bool direction) {
    const auto idx0 = _edge_index[eid0];
    const auto idx1 = _edge_index[eid1];
    SwapResult result;

    edge_t e[2] = {{_tail[idx0.second], _tail[idx0.first]}, {_tail[idx1.second], _tail[idx1.first]}};
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
            for (edgeid_t i = _first_tail[src]; i < _first_tail[src+1]; ++i) {
                if (_tail[i] == t[pos].second) {
                    result.conflictDetected[pos] = true;
                }
            }
        }
    }

    result.performed = !result.loop && !(result.conflictDetected[0] || result.conflictDetected[1]);

    if (result.performed) {
        // FIXME: ugly. Better ideas?
        auto oldPos = [&e, &idx0, &idx1](node_t v) {
            if (v == e[0].first) {
                return idx0.first;
            } else if (v == e[0].second) {
                return idx0.second;
            } else if (v == e[1].first) {
                return idx1.first;
            } else {
                return idx1.second;
            }
        };

        auto p = oldPos(t[0].first); // position, where t[0].second is written = opposite position where t[0].first was written
        _tail[p] = t[0].second;
        _edge_index[eid0].first = p;

        p = oldPos(t[0].second);
        _tail[p] = t[0].first;
        _edge_index[eid0].second = p;

        p = oldPos(t[1].first);
        _tail[p] = t[1].second;
        _edge_index[eid1].first = p;

        p = oldPos(t[1].second);
        _tail[p] = t[1].first;
        _edge_index[eid1].second = p;
    }

    result.normalize();

    return result;
}

IMGraph::IMEdgeStream IMGraph::getEdges() const {
    return IMEdgeStream(*this);
}



