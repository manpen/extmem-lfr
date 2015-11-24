#pragma once

#include <stxxl/vector>
#include "Swaps.h"

template <class EdgeVector = stxxl::vector<node_t>, class SwapVector = stxxl::vector<SwapDescriptor>>
class EdgeSwapTFP {
public:
   using debug_vector = stxxl::vector<SwapResult>;
   using edge_vector = EdgeVector;
   using swap_vector = SwapVector;

protected:
   EdgeVector & _edges;
   SwapVector & _swaps;

   debug_vector _results;

public:
   EdgeVectorTFP() = delete;
   EdgeVectorTFP(const EdgeVectorTFP &) = delete;

   //! Swaps are performed during constructor.
   //! @param edges  Edge vector changed in-place
   //! @param swaps  Read-only swap vector
   EdgeVectorTFP(edge_vector & edges, swap_vector & swaps)
      : _edges(edges), _swaps(swaps)
   {}

   //! The i-th entry of this vector corresponds to the i-th
   //! swap provided to the constructor
   debug_vector & debugVector() {
      return _result;
   }
};
