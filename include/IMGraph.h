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
        size_t pos;
        edge_t cur;
    public:
        IMEdgeStream(const IMGraph &graph) : _graph(graph), pos(0) {
            if (!empty()) cur = _graph.getEdge(pos);
        };


        const edge_t & operator * () const {
            return cur;
        };

        const edge_t * operator -> () const {
            return &cur;
        };

        IMEdgeStream & operator++ () {
            ++pos;
            if (!empty()) cur = _graph.getEdge(pos);
            return *this;
        }

        bool empty() const {
            return pos >= _graph.numEdges();
        };

    };
    struct node_ref {
        bool index_is_node;
        int_t index;
    };
    node_t _h;
    std::vector<node_t> _head;
    std::vector<edgeid_t> _first_head;
    std::vector<edgeid_t> _last_head;
    std::vector<std::pair<node_ref, node_ref>> _edge_index;
    std::vector<bool> _adjacency_matrix;
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
    void addEdge(const edge_t &e) {
        std::pair<node_ref, node_ref> idx = {{true, e.first}, {true, e.second}};
        if (e.first >= _h) {
            node_t i = e.first - _h;
            assert(_last_head[i] < _first_head[i+1]);
            _head[_last_head[i]] = e.second;
            idx.second = {false, _last_head[i]}; // warning: roles swapped
            ++_last_head[i];
        }
        if (e.second >= _h) {
            node_t j = e.second - _h;
            assert(_last_head[j] < _first_head[j+1]);
            _head[_last_head[j]] = e.first;
            idx.first = {false, _last_head[j]}; // warning: roles swapped
            ++_last_head[j];
        }
        if (e.first < _h && e.second < _h) {
            _adjacency_matrix[e.first*_h + e.second] = true;
            _adjacency_matrix[e.second*_h + e.first] = true;
        }
        _edge_index.push_back(idx);

        assert(getEdge(_edge_index.size()-1) == e);
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
        edge_t result= {idx.first.index, idx.second.index};

        if (!idx.first.index_is_node) {
            result.first = _head[result.first];
        }

        if (!idx.second.index_is_node) {
            result.second = _head[result.second];
        }

        return result;
    }

protected:
    /**
     * The degree of the given node if the node is not smaller than _h.
     *
     * @param u The node to get the degree for
     * @return The degree of @a u.
     */
    int_t degree(node_t u) const {
        assert(u >= _h && "Error, degree of the first nodes cannot be recovered");
        return  _last_head[u-_h] - _first_head[u-_h];
    }

public:
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
        if (u < _h && v < _h) return _adjacency_matrix[u*_h + v];

        if (u < _h) std::swap(u, v);
        // now u is definitely >= _h, but v could be < _h
        if (v >= _h && degree(u) > degree(v)) std::swap(u, v);

        for (edgeid_t i = _first_head[u-_h]; i < _last_head[u-_h]; ++i) {
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
    uint_t numEdges() const {
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
