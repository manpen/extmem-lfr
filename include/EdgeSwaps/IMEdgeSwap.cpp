#include <EdgeSwaps/IMEdgeSwap.h>
#include <IMGraphWrapper.h>

IMEdgeSwap::IMEdgeSwap(IMGraph &graph, const stxxl::vector< SwapDescriptor > &swaps) : _graph_wrapper(0), _graph(graph), _swaps(swaps) {}

IMEdgeSwap::IMEdgeSwap(stxxl::vector< edge_t > &edges, const stxxl::vector< SwapDescriptor > &swaps) : _graph_wrapper(new IMGraphWrapper(edges)), _graph(_graph_wrapper->getGraph()), _swaps(swaps) {}

IMEdgeSwap::~IMEdgeSwap() {
    if (_graph_wrapper != 0) {
        delete _graph_wrapper;
    }
}

void IMEdgeSwap::run() {
    swap_vector::bufreader_type swap_reader(_swaps);

#ifdef EDGE_SWAP_DEBUG_VECTOR
    // debug only
    debug_vector::bufwriter_type debug_vector_writer(_result);
#endif

    while (!swap_reader.empty()) {
        auto result = _graph.swapEdges(swap_reader->edges()[0], swap_reader->edges()[1], swap_reader->direction());
        stxxl::STXXL_UNUSED(result);

#ifdef EDGE_SWAP_DEBUG_VECTOR
        debug_vector_writer << result;
#endif

        ++swap_reader;
    }

    if (_graph_wrapper != 0) {
        _graph_wrapper->updateEdges();
    }
}



