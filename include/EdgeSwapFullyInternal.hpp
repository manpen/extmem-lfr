#pragma once

#include <map>
#include <algorithm>

#include <stxxl/vector>
#include <stxxl/sort>

#include <stx/btree_map>

#include <defs.h>
#include "Swaps.h"
#include "GenericComparator.h"
#include "TupleHelper.h"

/**
 * @brief Straight-forward (and slow) implementation of Edge Swaps used for cross-validation
 */
template <class EdgeVector = stxxl::vector<edge_t>, class SwapVector = stxxl::vector<SwapDescriptor>>
class EdgeSwapFullyInternal : public EdgeSwapBase {
public:
   using debug_vector = stxxl::vector<SwapResult>;
   using edge_vector = EdgeVector;
   using swap_vector = SwapVector;

protected:
   EdgeVector & _edges;
   SwapVector & _swaps;

   debug_vector _result;

   void _perform_swaps() {
      // copy edge list into btree
      stx::btree_map<edge_t, int_t> edge_idx_map;
      {
         typename EdgeVector::bufreader_type edge_reader(_edges);
         for(int_t eid=0; !edge_reader.empty(); ++edge_reader, ++eid) {
            edge_idx_map.insert(*edge_reader, eid);
         }
      }

      // perform swaps
      typename SwapVector::bufreader_type swap_reader(_swaps);
      typename debug_vector::bufwriter_type debug_vector_writer(_result);
      for(; !swap_reader.empty(); ++swap_reader) {
         auto & swap = *swap_reader;

         // read an swap edges
         const edge_t e0 = _edges[swap.edges()[0]];
         const edge_t e1 = _edges[swap.edges()[1]];
         edge_t se0, se1;
         std::tie(se0, se1) = _swap_edges(e0, e1, swap.direction());
         const auto s0_it = edge_idx_map.find(se0);
         const auto s1_it = edge_idx_map.find(se1);

         // check if there's are problem with this swap
         SwapResult res;
         res.loop = (se0.first == se0.second) || (se1.first == se1.second);
         res.edges[0] = se0;
         res.edges[1] = se1;
         res.conflictDetected[0] = (s0_it != edge_idx_map.end());
         res.conflictDetected[1] = (s1_it != edge_idx_map.end());
         res.performed = !res.loop && !res.conflictDetected[0] && !res.conflictDetected[1];
         res.normalize();
         debug_vector_writer << res;

         // update data structures
         if (res.performed) {
            _edges[swap.edges()[0]] = se0;
            _edges[swap.edges()[1]] = se1;
            auto ec0 = edge_idx_map.erase_one(e0);
            auto ec1 = edge_idx_map.erase_one(e1);
            (void)ec0;
            (void)ec1;
            assert(ec0);
            assert(ec1);
            edge_idx_map.insert(se0, swap.edges()[0]);
            edge_idx_map.insert(se1, swap.edges()[1]);
         }
      }

      debug_vector_writer.finish();

      // sort edge vector
      using comp = typename GenericComparator<edge_t>::Ascending;
      stxxl::sort(_edges.begin(), _edges.end(), comp(), 512 * IntScale::Mi);
   }


public:
   EdgeSwapFullyInternal() = delete;
   EdgeSwapFullyInternal(const EdgeSwapFullyInternal &) = delete;

   //! Swaps are performed during constructor.
   //! @param edges  Edge vector changed in-place
   //! @param swaps  Read-only swap vector
   EdgeSwapFullyInternal(edge_vector & edges, swap_vector & swaps) :
      EdgeSwapBase(),
      _edges(edges),
      _swaps(swaps)
   {}


   void run() {
      _perform_swaps();
   }

   //! The i-th entry of this vector corresponds to the i-th
   //! swap provided to the constructor
   debug_vector & debugVector() {
      return _result;
   }
};
