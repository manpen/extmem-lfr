#pragma once

#include <IMGraph.h>
#include <EdgeSwaps/EdgeSwapBase.h>
class IMGraphWrapper;

class IMEdgeSwap : public EdgeSwapBase {
private:
    IMGraphWrapper *_graph_wrapper;
    IMGraph &_graph;
#ifdef EDGE_SWAP_DEBUG_VECTOR
    typename debug_vector::bufwriter_type _debug_vector_writer;
#endif
public:
    IMEdgeSwap(IMGraph &graph, const stxxl::vector<SwapDescriptor>&);
    IMEdgeSwap(IMGraph &graph);

    /**
     * Initializes the IM edge swap implementation with the given edge vector that is converted into an internal memory graph.
     *
     * @param edges The given edge vector
     * @param swaps IGNORED, use push() instead
     */
    IMEdgeSwap(stxxl::vector<edge_t> &edges, const stxxl::vector<SwapDescriptor>&);

    IMEdgeSwap(stxxl::vector<edge_t> &edges);

    ~IMEdgeSwap();

    //! Executes a single swap
    void push(const swap_descriptor& swap);

    //! Writes out changes into edge vector if given in constructor, otherwise does nothing. Further swaps can still be given aftwards.
    void flush();

    //! Writes out changes into edge vector if given in constructor; finishes writing the debug vector when enabled.
    void run();
};

template <>
struct EdgeSwapTrait<IMEdgeSwap> {
    static bool swapVector() {return false;}
    static bool pushableSwaps() {return true;}
    static bool pushableSwapBuffers() {return false;}
};