#include <gtest/gtest.h>

#include <stxxl/vector>
#include <stxxl/stream>

#include <PowerlawDegreeSequence.h>
#include <DistributionCount.h>
#include <HavelHakimiGeneratorRLE.h>

#include <SwapGenerator.hpp>

#include "EdgeSwapInternalSwaps.hpp"
#include "EdgeSwapTFP.hpp"
#include "EdgeSwapFullyInternal.hpp"

namespace {
   using EdgeVector = stxxl::vector<edge_t>;
   using SwapVector = stxxl::vector<SwapDescriptor>;


   template <class Algo>
   class TestEdgeSwapCross : public ::testing::Test {
   protected:
      template <class List>
      void _print_list(List & list, bool show = true) const {
         if (!show) return;
         unsigned int i = 0;
         for(auto &e : list)
            std::cout << i++ << " " << e << std::endl;
      }

      EdgeVector _generate_hh_graph(int_t number_of_nodes) const {
         EdgeVector edges;

         PowerlawDegreeSequence degreeSequence(1, number_of_nodes / 3, -2.0, number_of_nodes);

         DistributionCount<PowerlawDegreeSequence> dcount(degreeSequence);
         HavelHakimiGeneratorRLE<DistributionCount<PowerlawDegreeSequence>> hhgenerator(dcount);


         edges.resize(hhgenerator.maxEdges());
         auto endIt = stxxl::stream::materialize(hhgenerator, edges.begin());
         edges.resize(endIt - edges.begin());


         return edges;
      }

      SwapVector _generate_swaps(int_t number_of_swaps, int_t edges_in_graph) const {
         SwapVector swaps(number_of_swaps);

         SwapGenerator sgen(number_of_swaps, edges_in_graph);
         stxxl::stream::materialize(sgen, swaps.begin());

         return swaps;
      }
   };

   using TestEdgeSwapCrossImplementations = ::testing::Types <
      EdgeSwapInternalSwaps<EdgeVector, SwapVector>,
      EdgeSwapTFP<EdgeVector, SwapVector>
   >;

   TYPED_TEST_CASE(TestEdgeSwapCross, TestEdgeSwapCrossImplementations);

   TYPED_TEST(TestEdgeSwapCross, againstFullyInternal) {
      bool debug_this_test = false;
      using EdgeSwapAlgoUnderTest = TypeParam;

      auto edges = this->_generate_hh_graph(100);
      auto swaps = this->_generate_swaps(edges.size(), edges.size());

      std::cout << " Using graph with " << edges.size() << " edges and request " << swaps.size() << "swaps" << std::endl;

      EdgeVector edges_ref(edges);
      SwapVector swaps_ref(swaps);

      EdgeSwapFullyInternal<EdgeVector, SwapVector> es_ref(edges_ref, swaps_ref);
      EdgeSwapAlgoUnderTest es_test(edges, swaps);
      es_ref.run();
      es_test.run();

      auto & res_ref = es_ref.debugVector();
      auto & res_test = es_test.debugVector();

      ASSERT_EQ(res_ref.size(), swaps.size());
      ASSERT_EQ(res_test.size(), swaps.size());
      ASSERT_EQ(edges.size(), edges_ref.size());

      // compare debug vectors
      int_t performed = 0;
      for(uint_t i = 0; i < swaps.size(); i++) {
         auto & rr = res_ref[i];
         auto & rt = res_test[i];

         ASSERT_EQ(rr.performed, rt.performed) << "i=" << i << " " << rr << " " << rt;
         ASSERT_EQ(rr.loop, rt.loop) << "i=" << i << " " << rr << " " << rt;

         performed += rr.performed;
      }

      std::cout << " Performed " << performed << " swaps and skipped " << (swaps.size() - performed) << std::endl;

      // compare edges
      for(uint_t i = 0; i < edges.size(); i++) {
         auto & er = edges_ref[i];
         auto & et = edges[i];

         ASSERT_EQ(er, et) << "i=" << i << "er: " << er << " et: " << et;
      }
   }
}
