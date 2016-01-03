/**
 * @file
 * @brief Swap Descriptor and Results
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */
#pragma once
#include <defs.h>
#include <cassert>

using swapid_t = int64_t;

/**
 * @brief Store edge ids and direction describing a swap
 * @todo We could reduce the size for EM using the stxxl::uintXX types.
 * Additionally, we can implicitly encode the direction using the order of e1 and e2
 */
class SwapDescriptor {
   edgeid_t _edges[2];
   bool _direction;
   
public:
   SwapDescriptor() : _edges{0, 0}, _direction(false) {}

   //! Edges must be disjoint, i.e. e1 != e2
   SwapDescriptor(edgeid_t e1, edgeid_t e2, bool dir)
      : _edges{e1, e2}, _direction(dir)
   {
      assert(e1 != e2);
      if (e1 > e2) std::swap(_edges[0], _edges[1]);
   }

   //! Constant array of two edge ids; the first ed
   const edgeid_t* edges() const {return _edges;}

   /**
    * Indicate swap direction:  <br />
    * direction == false: (v1, v3) and (v2, v4)<br />
    * direction == true : (v2, v3) and (v1, v4)
    */
   bool direction() const {return _direction;}
};

inline std::ostream &operator<<(std::ostream &os, SwapDescriptor const &m) {
   return os << "{swap edges " << m.edges()[0] << " and " << m.edges()[1] << " dir " << m.direction() << "}";
}

/**
 * @brief Results of an attempted swap
 *
 * Contains information whether a swap was performed or indicates
 * reasons why it was not.
 */
struct SwapResult {
   //! Swap was performed
   bool performed;

   //! Swap was not performed since it would produce at least one self-loop
   bool loop;

   //! use a self-loop to indicate, that the information is invalid
   edge_t edges[2];

   //! Indicates that the edge(s) stored in vertices prevented the swap.
   //! Field may only be asserted in case the corresponding edge is valid (no self-loop)
   bool conflictDetected[2];

   /**
    * Orders (v0, v1) and (v2, v3) with v0 <= v1, v2 <= v3.
    * In case only one conflict was detected, it will be moved to the first entry,
    * otherwise the edges are arranged s.t. v0 <= v3
    */
   void normalize() {
      edges[0].normalize();
      edges[0].normalize();

      if (edges[1] > edges[0]) {
         std::swap(edges[0], edges[1]);
         std::swap(conflictDetected[0], conflictDetected[1]);
      }
   }
};

inline std::ostream &operator<<(std::ostream &os, SwapResult const &m) {
   return os <<
      "{swap-result "
          "perf:" << m.performed << ", "
          "loop:" << m.loop << ", "
          "edge0: (" << m.edges[0].first << "," << m.edges[0].second << ") "
          "confl0: " << m.conflictDetected[0] << ", "
          "edge1: (" << m.edges[1].first << "," << m.edges[1].second << ") "
          "confl1: " << m.conflictDetected[1] <<
      "}";
}
