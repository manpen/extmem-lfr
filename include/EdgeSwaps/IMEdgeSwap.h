#pragma once

#include <IMGraph.h>
#include <EdgeSwaps/EdgeSwapBase.h>
#include <IMGraphWrapper.h>
#include <stxxl/vector>

class IMEdgeSwap : public EdgeSwapBase {
private:
    IMGraphWrapper *_graph_wrapper;
    IMGraph &_graph;
    const swap_vector& _swaps;
public:
    IMEdgeSwap(IMGraph &graph, const stxxl::vector<SwapDescriptor>& swaps) : _graph_wrapper(0), _graph(graph), _swaps(swaps) {};

    IMEdgeSwap(stxxl::vector<edge_t> &edges, const stxxl::vector<SwapDescriptor>& swaps) : _graph_wrapper(new IMGraphWrapper(edges)), _graph(_graph_wrapper->getGraph()), _swaps(swaps) {};

    ~IMEdgeSwap() {
        if (_graph_wrapper != 0) {
            delete _graph_wrapper;
        }
    };

    void run() {
        swap_vector::bufreader_type swap_reader(_swaps);

#ifdef EDGE_SWAP_DEBUG_VECTOR
        // debug only
        debug_vector::bufwriter_type debug_vector_writer(_result);
#endif

        while (!swap_reader.empty()) {
            auto result = _graph.swapEdges(swap_reader->edges()[0], swap_reader->edges()[1], swap_reader->direction());

#ifdef EDGE_SWAP_DEBUG_VECTOR
            debug_vector_writer << result;
#endif

            ++swap_reader;
        }

        if (_graph_wrapper != 0) {
            _graph_wrapper->updateEdges();
        }
    };
};
