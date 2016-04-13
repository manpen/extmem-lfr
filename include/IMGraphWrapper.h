#pragma once

#include <IMGraph.h>
#include <stxxl/sorter>
#include <GenericComparator.h>
#include <EdgeStream.h>

class IMGraphWrapper {
private:
    EdgeStream& _edges;

    std::vector<degree_t> _degrees;
    IMGraph* _graph;
public:
    IMGraphWrapper(EdgeStream &edges) : _edges(edges), _graph(0) {
        {
            node_t maxId = -1;
            while (!edges.empty()) {
                auto & edge = *edges;

                node_t m = std::max(edge.first, edge.second);
                if (m > maxId) {
                    _degrees.resize(m+1);
                    maxId = m;
                }

                ++_degrees[edge.first];
                ++_degrees[edge.second];
                ++edges;
            }

            edges.rewind();
        }

        _graph = new IMGraph(_degrees);

        {
            while (!edges.empty()) {
                _graph->addEdge(*edges);
                ++edges;
            }

            edges.rewind();
        }
    };

    ~IMGraphWrapper() {
        if (_graph != 0) {
            delete _graph;
        }
    };

    IMGraph& getGraph() { return *_graph; };

    void updateEdges() {
        using comp = typename GenericComparator<edge_t>::Ascending;
        stxxl::sorter<edge_t, comp> edge_sorter(comp(), SORTER_MEM);
        for (auto it = _graph->getEdges(); !it.empty(); ++it) {
            edge_sorter.push(*it);
        }
        edge_sorter.sort();

        _edges.clear();
        for(; !edge_sorter.empty(); ++edge_sorter)
            _edges.push(*edge_sorter);
        _edges.consume();
    };
};