#pragma once
#include <vector>
#include <cassert>
#include <defs.h>
#include <parallel/algorithm>
#include <stxxl/random>
#include <EdgeSwaps/EdgeSwapBase.h>

class IMGraph : private EdgeSwapBase {
private:
    class IMEdgeStream {
    private:
        const IMGraph &_graph;
        node_t u;
        size_t pos;
        edge_t cur;

        void findNext() {
            for (; pos < _graph._head.size(); ++pos) {
                while (pos < _graph._head.size() && pos == static_cast<size_t>(_graph._last_head[u])) {
                    ++u;
                    pos = _graph._first_head[u];
                }

                if (pos < _graph._head.size() && u <= _graph._head[pos]) {
                    cur = {u, _graph._head[pos]};
                    return;
                }
            }
        }
    public:
        IMEdgeStream(const IMGraph &graph) : _graph(graph), u(0), pos(0) {
            findNext();
        };


        const edge_t & operator * () const {
            return cur;
        };

        const edge_t * operator -> () const {
            return &cur;
        };

        IMEdgeStream & operator++ () {
            ++pos;
            findNext();
            return *this;
        }

        bool empty() const {
            return pos >= _graph._head.size();
        };

    };
    std::vector<node_t> _head;
    std::vector<edgeid_t> _first_head;
    std::vector<edgeid_t> _last_head;
    std::vector<std::pair<edgeid_t, edgeid_t>> _edge_index;
    stxxl::random_number64 _random_integer;
public:
    /**
     * Constructs a new internal memory graph.
     */
    IMGraph(const std::vector<degree_t> &degreeSequence);

    /**
     * Adds a new edge to the graph.
     *
     * The caller must ensure that the edge does not exist yet. The edge is added in both directions.
     *
     * @param e The edge to add
     */
    void addEdge(edge_t e) {
        assert(_last_head[e.first] < _first_head[e.first+1] && _last_head[e.second] < _first_head[e.second+1]);
        _head[_last_head[e.first]] = e.second;
        _head[_last_head[e.second]] = e.first;
        _edge_index.emplace_back(_last_head[e.first], _last_head[e.second]);
        ++_last_head[e.first];
        ++_last_head[e.second];
    }

    /**
     * Get a random edge id.
     *
     * @return A random edge id
     */
    edgeid_t randomEdge() const {
        return _random_integer(_edge_index.size());
    }

    /**
     * Get the edge that is identified by the given edge id.
     *
     * @param eid The id fo the edge to return
     * @return The requested edge
     */
    edge_t getEdge(edgeid_t eid) const {
        auto &idx = _edge_index[eid];
        return {_head[idx.second], _head[idx.first]};
    }

    /**
     * The degree of the given node.
     *
     * @param u The node to get the degree for
     * @return The degree of @a u.
     */
    int_t degree(node_t u) const {
        return  _last_head[u] - _first_head[u];
    }

    /**
     * Checks if the given edge exists.
     *
     * Running time is linear in the size of the smaller degree of the two nodes.
     *
     * @param u The source node
     * @param v The target node
     * @return If the edge exists
     */
     bool hasEdge(node_t u, node_t v) const {
        if (degree(u) > degree(v)) std::swap(u, v);
        for (edgeid_t i = _first_head[u]; i < _last_head[u]; ++i) {
            if (UNLIKELY(_head[i] == v)) {
                return true;
            }
        }
        return false;
     }

    /**
     * Get the number of edges the graph has.
     *
     * @return The number of edges.
     */
    int_t numEdges() const {
        return _edge_index.size();
    }

    /**
     * Swap the two edges that are identified by the given edge ids.
     *
     * @param eid0 The id of the first swap candidate
     * @param eid1 The id of the second swap candidate
     * @return If the swap was successfull, i.e. did not create any conflict.
     */
    SwapResult swapEdges(const edgeid_t eid0, const edgeid_t eid1, bool direction);

    /**
     * Get a stream of normalized edges. The edges are unsorted.
     *
     * @return An implementation of the STXXL stream interface with all normalized edges (unsorted).
     */
    IMEdgeStream getEdges() const;
};
