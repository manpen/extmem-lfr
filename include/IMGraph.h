#pragma once
#include <vector>
#include <cassert>
#include <defs.h>
#include <parallel/algorithm>
#include <stxxl/random>

class IMGraph {
private:
    class IMEdgeStream {
    private:
        const std::vector<edge_t> &_edges;
        std::vector<edge_t>::const_iterator _it;
    public:
        IMEdgeStream(const std::vector<edge_t> &edges) : _edges(edges), _it(_edges.begin()) {
            for (; _it != _edges.end() && _it->first > _it->second; ++_it) ;
        };


        const edge_t & operator * () const {
            return *_it;
        };

        const edge_t * operator -> () const {
            return &*_it;
        };

        IMEdgeStream & operator++ () {
            for (++_it; _it != _edges.end() && _it->first > _it->second; ++_it) ;
            return *this;
        }

        bool empty() const {
            return _it == _edges.end();
        };

    };
    std::vector<edge_t> _edges;
    std::vector<edgeid_t> _first_edge;
    bool _is_sorted;
    stxxl::random_number64 _random_integer;
public:
    /**
     * Constructs a new internal memory graph.
     */
    IMGraph();

    /**
     * Adds a new edge to the graph.
     *
     * The caller must ensure that the edge does not exist yet. The edge is added in both directions.
     *
     * @param e The edge to add
     */
    void addEdge(edge_t e) {
        _edges.emplace_back(e);
        _edges.push_back(edge_t {e.second, e.first});
        _is_sorted = false;
    }

    /**
     * Sort the edge such that swaps can be performed.
     */
    void sort();

    /**
     * Get a random edge id.
     *
     * @return A random edge id
     */
    edgeid_t randomEdge() const {
        return _random_integer(_edges.size());
    }

    /**
     * Get the edge that is identified by the given edge id.
     *
     * @param eid The id fo the edge to return
     * @return The requested edge
     */
    edge_t getEdge(edgeid_t eid) const {
        return _edges[eid];
    }

    /**
     * Get the number of edges the graph has.
     *
     * @return The number of edges.
     */
    int_t numEdges() const {
        return _edges.size() / 2;
    }

    /**
     * Swap the two edges that are identified by the given edge ids.
     *
     * @param eid0 The id of the first swap candidate
     * @param eid1 The id of the second swap candidate
     * @return If the swap was successfull, i.e. did not create any conflict.
     */
    bool swapEdges(edgeid_t eid0, edgeid_t eid1);

    /**
     * Get a stream of normalized edges. The edges are unsorted.
     *
     * @return An implementation of the STXXL stream interface with all normalized edges (unsorted).
     */
    IMEdgeStream getEdges() const;
};
