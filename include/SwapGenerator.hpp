/**
 * @file
 * @brief Swap Descriptor and Results
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */

#pragma once
#include <stxxl/random>
#include "Swaps.h"

class SwapGenerator {
public:
   using value_type = SwapDescriptor;

protected:
   const int64_t _number_of_edges_in_graph;
   const int64_t _requested_number_of_swaps;

   int64_t _current_number_of_swaps;
   value_type _current_swap;

   stxxl::random_number32 _random_flag;
   stxxl::random_number64 _random_integer;

public:
   SwapGenerator(int64_t number_of_swaps, int64_t edges_in_graph)
      : _number_of_edges_in_graph(edges_in_graph)
      , _requested_number_of_swaps(number_of_swaps)
      , _current_number_of_swaps(0)
   {
      assert(_number_of_edges_in_graph > 1);
      ++(*this);
   }

//! @name STXXL Streaming Interface
//! @{
   bool empty() const {return _current_number_of_swaps > _requested_number_of_swaps;}
   const value_type & operator*() const {return _current_swap;}

   SwapGenerator& operator++() {
      _current_number_of_swaps++;

      while (1) {
         // generate two disjoint random edge ids
         edgeid_t e1 = _random_integer(_number_of_edges_in_graph);
         edgeid_t e2 = _random_integer(_number_of_edges_in_graph);
         if (e1 == e2) continue;

         // direction flag
         bool dir = _random_flag(2);

         _current_swap = {e1, e2, dir};
         return *this;
      }
   }
//! @}
};
