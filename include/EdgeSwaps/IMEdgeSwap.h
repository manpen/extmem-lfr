#pragma once

#include <IMGraph.h>
#include <EdgeSwaps/EdgeSwapBase.h>
#include <stxxl/vector>

class IMEdgeSwap : public EdgeSwapBase {
private:
    IMGraph &_graph;
    const swap_vector& _swaps;
public:
    IMEdgeSwap(IMGraph &graph, const stxxl::vector<SwapDescriptor>& swaps) : _graph(graph), _swaps(swaps) {};

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
    };
};
