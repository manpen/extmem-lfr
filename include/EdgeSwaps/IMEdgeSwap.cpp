#include <EdgeSwaps/IMEdgeSwap.h>
#include <IMGraphWrapper.h>

IMEdgeSwap::IMEdgeSwap(IMGraph &graph, const stxxl::vector< SwapDescriptor > &) : IMEdgeSwap(graph)
{}

IMEdgeSwap::IMEdgeSwap(IMGraph &graph) : _graph_wrapper(0), _graph(graph)
#ifdef EDGE_SWAP_DEBUG_VECTOR
        , _debug_vector_writer(_result)
#endif
{ }


IMEdgeSwap::IMEdgeSwap(stxxl::vector< edge_t > &edges, const stxxl::vector< SwapDescriptor > &) : IMEdgeSwap(edges)
{}

IMEdgeSwap::IMEdgeSwap(stxxl::vector< edge_t > &edges) : _graph_wrapper(new IMGraphWrapper(edges)), _graph(_graph_wrapper->getGraph())
#ifdef EDGE_SWAP_DEBUG_VECTOR
        , _debug_vector_writer(_result)
#endif
{}


IMEdgeSwap::~IMEdgeSwap() {
    if (_graph_wrapper != 0) {
        delete _graph_wrapper;
    }
}

void IMEdgeSwap::push(const EdgeSwapBase::swap_descriptor &swap) {
    auto result = _graph.swapEdges(swap.edges()[0], swap.edges()[1], swap.direction());
    stxxl::STXXL_UNUSED(result);

#ifdef EDGE_SWAP_DEBUG_VECTOR
    _debug_vector_writer << result;
#endif

}

void IMEdgeSwap::run() {
    if (_graph_wrapper != 0) {
        _graph_wrapper->updateEdges();
    }

#ifdef EDGE_SWAP_DEBUG_VECTOR
        _debug_vector_writer.finish();
#endif
}



