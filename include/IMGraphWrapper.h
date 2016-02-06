#pragma once

#include <IMGraph.h>
#include <stxxl/vector>
#include <stxxl/sort>
#include <GenericComparator.h>

class IMGraphWrapper {
private:
    stxxl::vector<edge_t>& _edges;
    std::vector<degree_t> _degrees;
    IMGraph* _graph;
public:
    IMGraphWrapper(stxxl::vector<edge_t> &edges) : _edges(edges), _graph(0) {
        {
            stxxl::vector<edge_t>::bufreader_type edge_reader(edges);
            node_t maxId = -1;
            while (!edge_reader.empty()) {
                node_t m = std::max(edge_reader->first, edge_reader->second);
                if (m > maxId) {
                    _degrees.resize(m+1);
                    maxId = m;
                }

                ++_degrees[edge_reader->first];
                ++_degrees[edge_reader->second];
                ++edge_reader;
            }
        }

        _graph = new IMGraph(_degrees);

        {
            stxxl::vector<edge_t>::bufreader_type edge_reader(edges);
            while (!edge_reader.empty()) {
                _graph->addEdge(*edge_reader);
                ++edge_reader;
            }
        }
    };

    ~IMGraphWrapper() {
        if (_graph != 0) {
            delete _graph;
        }
    };

    IMGraph& getGraph() { return *_graph; };

    void updateEdges() {
        _edges.clear();
        stxxl::vector<edge_t>::bufwriter_type writer(_edges);
        for (auto it = _graph->getEdges(); !it.empty(); ++it) {
            writer << *it;
        }
        writer.finish();
        // sort edge vector
        using comp = typename GenericComparator<edge_t>::Ascending;
        stxxl::sort(_edges.begin(), _edges.end(), comp(), 512 * IntScale::Mi);
    };
};