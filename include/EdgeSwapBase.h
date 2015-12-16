#pragma once
#include <Swaps.h>
#include <defs.h>

class EdgeSwapBase {
protected:
   std::pair<edge_t, edge_t> _swap_edges(const edge_t & e0, const edge_t & e1, bool direction) const {
      edge_t t0, t1;
      if (direction) {
         if (e0.second < e1.first) {
            t0 = {e0.second, e1.first};
         } else {
            t0 = {e1.first, e0.second};
         }
         if (e0.first < e1.second) {
            t1 = {e0.first, e1.second};
         } else {
            t1 = {e1.second, e0.first};
         }
      } else {
         if (e1.first < e0.first) {
            t0 = {e1.first, e0.first};
         } else {
            t0 = {e0.first, e1.first};
         }
         if (e0.second < e1.second) {
            t1 = {e0.second, e1.second};
         } else {
            t1 = {e1.second, e0.second};
         }
      }

      return std::make_pair(t0, t1);
   }
};
