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
            for (; pos < _graph._tail.size(); ++pos) {
                while (pos == _graph._last_tail[u]) {
                    ++u;
                    pos = _graph._first_tail[u];
                }

                if (u <= _graph._tail[pos]) {
                    cur = {u, _graph._tail[pos]};
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
            return pos == _graph._tail.size();
        };

    };
    std::vector<node_t> _tail;
    std::vector<edgeid_t> _first_tail;
    std::vector<edgeid_t> _last_tail;
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
        assert(_last_tail[e.first] < _first_tail[e.first+1] && _last_tail[e.second] < _first_tail[e.second+1]);
        _tail[_last_tail[e.first]] = e.second;
        _tail[_last_tail[e.second]] = e.first;
        _edge_index.emplace_back(_last_tail[e.first], _last_tail[e.second]);
        ++_last_tail[e.first];
        ++_last_tail[e.second];
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
        return {_tail[idx.second], _tail[idx.first]};
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
