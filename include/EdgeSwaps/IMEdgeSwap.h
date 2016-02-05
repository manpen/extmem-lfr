#pragma once

#include <IMGraph.h>
#include <EdgeSwaps/EdgeSwapBase.h>
class IMGraphWrapper;

class IMEdgeSwap : public EdgeSwapBase {
private:
    IMGraphWrapper *_graph_wrapper;
    IMGraph &_graph;
    const swap_vector& _swaps;
public:
    IMEdgeSwap(IMGraph &graph, const stxxl::vector<SwapDescriptor>& swaps);

    IMEdgeSwap(stxxl::vector<edge_t> &edges, const stxxl::vector<SwapDescriptor>& swaps);

    ~IMEdgeSwap();

    void run();
};
