#include <IMGraph.h>

IMGraph::IMGraph() : _is_sorted(false) {};


void IMGraph::sort() {
    if (_edges.empty()) {
        _is_sorted = true;
        return;
    }

    __gnu_parallel::sort(_edges.begin(), _edges.end());

    _first_edge.clear();
    _first_edge.resize(_edges.back().first + 2);
    node_t u = -1;
    for (edgeid_t eid = 0; eid < static_cast<edgeid_t>(_edges.size()); ++eid) {
        if (_edges[eid].first != u) {
            u = _edges[eid].first;
            _first_edge[u] = eid;
        }
    }
    _first_edge.back() = _edges.size();

    _is_sorted = true;
}

bool IMGraph::swapEdges(edgeid_t eid0, edgeid_t eid1) {
    assert(_is_sorted);
    edge_t e[2] = {_edges[eid0], _edges[eid1]};
    edge_t t[2] = {{e[0].first, e[1].second}, {e[1].first, e[0].second}};

    // check for conflict: loop
    if (t[0].first == t[0].second || t[1].first == t[1].second) return false;
    if (t[0].second == t[1].second) return true; // swap would be a no-op!

    // find index of reverse edge and possible conflict edge
    edge_t tr[2] = {{t[0].second, t[0].first}, {t[1].second, t[1].first}};
    edge_t er[2] = {{e[1].second, e[1].first}, {e[0].second, e[0].first}};
    edgeid_t eid_r[2] = {-1, -1};

    for (unsigned char pos = 0; pos < 2; ++pos) {
        node_t src = tr[pos].first;
        assert(src == er[pos].first);
        for (edgeid_t i = _first_edge[src]; i < _first_edge[src+1]; ++i) {
            if (_edges[i].second == er[pos].second) {
                eid_r[pos] = i;
            }

            if (_edges[i].second == tr[pos].second) {
                return false;
            }
        }
    }

    assert(eid_r[0] != -1 && eid_r[1] != -1);

    _edges[eid0] = t[0];
    _edges[eid1] = t[1];

    _edges[eid_r[0]] = tr[0];
    _edges[eid_r[1]] = tr[1];

    return true;
}

IMGraph::IMEdgeStream IMGraph::getEdges() const {
    return IMEdgeStream(_edges);
}



