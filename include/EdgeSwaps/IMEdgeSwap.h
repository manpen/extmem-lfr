#pragma once

#include <IMGraph.h>

class IMEdgeSwap {
private:
    IMGraph &_graph;
    int_t _numSwaps;
public:
    IMEdgeSwap(IMGraph &graph, int_t numSwaps) : _graph(graph), _numSwaps(numSwaps) {};

    void run() {
        for (int_t i = 0; i < _numSwaps; ++i) {
            _graph.swapEdges(_graph.randomEdge(), _graph.randomEdge());
        }
    };
};
