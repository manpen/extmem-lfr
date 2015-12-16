#include <gtest/gtest.h>

#include <stxxl/vector>
#include "EdgeSwapTFP.hpp"

namespace {
   using EdgeList = stxxl::vector<edge_t>;
   using SwapList = stxxl::vector<SwapDescriptor>;


   template <class Algo>
   class TestEdgeSwap : public ::testing::Test {
   protected:
      template <class List>
      void _print_list(List & list, bool show = true) {
         if (!show) return;
         unsigned int i = 0;
         for(auto &e : list)
            std::cout << i++ << " " << e << std::endl;
      }
   };

   using TestEdgeSwapImplementations = ::testing::Types <
      EdgeSwapTFP<EdgeList, SwapList>
   >;

   TYPED_TEST_CASE(TestEdgeSwap, TestEdgeSwapImplementations);

   TYPED_TEST(TestEdgeSwap, noConflicts) {
      bool debug_this_test = false;
      using EdgeSwapAlgo = TypeParam;

      EdgeList edge_list;
      edge_list.push_back({0, 1});
      edge_list.push_back({1, 3});
      edge_list.push_back({2, 3});
      edge_list.push_back({3, 4});

      SwapList swap_list;
      swap_list.push_back({0, 2, true});
      swap_list.push_back({0, 3, true});
      swap_list.push_back({2, 3, false});
      swap_list.push_back({0, 2, true});


      EdgeSwapAlgo algo(edge_list, swap_list, debug_this_test);

      this->_print_list(edge_list, debug_this_test);

      auto & debug = algo.debugVector();
      this->_print_list(debug, debug_this_test);

      ASSERT_TRUE(debug[0].performed);
      ASSERT_TRUE(debug[1].performed);
      ASSERT_TRUE(debug[2].performed);
      ASSERT_TRUE(debug[3].performed);

      ASSERT_EQ(edge_list[0], edge_t(0, 3));
      ASSERT_EQ(edge_list[1], edge_t(1, 2));
      ASSERT_EQ(edge_list[2], edge_t(1, 3));
      ASSERT_EQ(edge_list[3], edge_t(3, 4));
   }
   
   /*
    * This test is only useful for debugging the dependency chain
    * as it has a very clean dependency structure:
    * Swap 0 may yield edges (1,2) (0,3) (0,1) (2,3)
    * Swap 1 may yield edges (5,6) (4,7) (4,5) (6,7)
    * Swap 2 may yield edges (1,4) (0,5) (1,5) (0,6) (2,4) (1,5) (2,5) (1,6) (0,1) (1,2) (4,5) (5,6)
    * Swap 3 may yield edges (3,4) (0,7) (3,6) (0,7) (3,4) (2,7) (3,6) (2,7) (0,3) (2,3) (4,7) (6,7)
    */
   TYPED_TEST(TestEdgeSwap, configs) {
      bool debug_this_test = false;
      using EdgeSwapAlgo = TypeParam;
      EdgeList edge_list;

      edge_list.push_back({0, 1});
      edge_list.push_back({2, 3});
      edge_list.push_back({4, 5});
      edge_list.push_back({6, 7});

      SwapList swap_list;

      swap_list.push_back({0, 1, true});
      swap_list.push_back({2, 3, true});
      swap_list.push_back({0, 2, true});
      swap_list.push_back({1, 3, true});

      EdgeSwapAlgo algo(edge_list, swap_list, debug_this_test);

      this->_print_list(edge_list, debug_this_test);

      auto & debug = algo.debugVector();
      this->_print_list(debug, debug_this_test);

      ASSERT_TRUE(debug[0].performed);
      ASSERT_TRUE(debug[1].performed);
      ASSERT_TRUE(debug[2].performed);
      ASSERT_TRUE(debug[3].performed);

      ASSERT_EQ(edge_list[0], edge_t(0, 7));
      ASSERT_EQ(edge_list[1], edge_t(1, 6));
      ASSERT_EQ(edge_list[2], edge_t(2, 5));
      ASSERT_EQ(edge_list[3], edge_t(3, 4));
   }

   TYPED_TEST(TestEdgeSwap, conflicts) {
      bool debug_this_test = false;
      using EdgeSwapAlgo = TypeParam;

      EdgeList edge_list;
      edge_list.push_back({0, 1});
      edge_list.push_back({1, 2});
      edge_list.push_back({2, 3});
      edge_list.push_back({3, 4});


      SwapList swap_list;

      swap_list.push_back({0, 1, true});
      swap_list.push_back({0, 2, true});
      swap_list.push_back({0, 2, false});

      EdgeSwapAlgo algo(edge_list, swap_list, debug_this_test);

      this->_print_list(edge_list, debug_this_test);

      auto & debug = algo.debugVector();
      this->_print_list(debug, debug_this_test);

      ASSERT_FALSE(debug[0].performed);
      ASSERT_TRUE (debug[0].loop);
      ASSERT_FALSE(debug[1].performed);
      ASSERT_TRUE (debug[2].performed);

   }
}