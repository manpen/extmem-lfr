#include <gtest/gtest.h>

#include <stxxl/vector>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/EdgeSwapTFP.h>
#include <EdgeSwaps/EdgeSwapFullyInternal.h>
#include <EdgeSwaps/IMEdgeSwap.h>


#ifdef EDGE_SWAP_DEBUG_VECTOR
namespace {
   using EdgeVector = stxxl::vector<edge_t>;
   using SwapVector = stxxl::vector<SwapDescriptor>;

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
      EdgeSwapInternalSwaps,
      EdgeSwapTFP::EdgeSwapTFP,
      EdgeSwapFullyInternal<EdgeVector, SwapVector>,
      IMEdgeSwap
   >;

   TYPED_TEST_CASE(TestEdgeSwap, TestEdgeSwapImplementations);

   TYPED_TEST(TestEdgeSwap, noConflicts) {
      bool debug_this_test = false;
      using EdgeSwapAlgo = TypeParam;

      EdgeVector edge_list;
      edge_list.push_back({0, 1});
      edge_list.push_back({1, 3});
      edge_list.push_back({2, 3});
      edge_list.push_back({3, 4});

      SwapVector swap_list;
      swap_list.push_back({0, 2, true});
      swap_list.push_back({0, 3, true});
      swap_list.push_back({2, 3, false});
      swap_list.push_back({0, 2, true});

      EdgeSwapAlgo algo(edge_list, swap_list);
      algo.setDisplayDebug(debug_this_test);

      if (EdgeSwapTrait<EdgeSwapAlgo>::pushableSwaps()) {
         for (auto &s : swap_list)
            algo.push(s);
      }

      algo.run();

      this->_print_list(edge_list, debug_this_test);
 
      auto & debug = algo.debugVector();
      this->_print_list(debug, debug_this_test);

      for(unsigned int i=0; i < swap_list.size(); i++) {
         ASSERT_TRUE(debug[i].performed) << "with i=" << i << " and " << debug[i];
      }

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
      EdgeVector edge_list;

      edge_list.push_back({0, 1});
      edge_list.push_back({2, 3});
      edge_list.push_back({4, 5});
      edge_list.push_back({6, 7});

      SwapVector swap_list;

      swap_list.push_back({0, 1, true});
      swap_list.push_back({2, 3, true});
      swap_list.push_back({0, 2, true});
      swap_list.push_back({1, 3, true});

      EdgeSwapAlgo algo(edge_list, swap_list);
      algo.setDisplayDebug(debug_this_test);

      if (EdgeSwapTrait<EdgeSwapAlgo>::pushableSwaps()) {
         for (auto &s : swap_list)
            algo.push(s);
      }

      algo.run();

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
      bool debug_this_test = true;
      using EdgeSwapAlgo = TypeParam;

      EdgeVector edge_list;
      edge_list.push_back({0, 1});
      edge_list.push_back({1, 2});
      edge_list.push_back({2, 3});
      edge_list.push_back({3, 4});


      SwapVector swap_list;

      swap_list.push_back({0, 1, true});
      swap_list.push_back({0, 2, true});
      swap_list.push_back({0, 2, false});

      EdgeSwapAlgo algo(edge_list, swap_list);
      algo.setDisplayDebug(debug_this_test);

      if (EdgeSwapTrait<EdgeSwapAlgo>::pushableSwaps()) {
         for (auto &s : swap_list)
            algo.push(s);
      }


      algo.run();

      this->_print_list(edge_list, debug_this_test);

      auto & debug = algo.debugVector();
      this->_print_list(debug, debug_this_test);

      ASSERT_FALSE(debug[0].performed);
      ASSERT_TRUE (debug[0].loop);
      ASSERT_FALSE(debug[1].performed);
      ASSERT_TRUE (debug[2].performed);

   }

   TYPED_TEST(TestEdgeSwap, existencePropagation) {
      bool debug_this_test = true;
       using EdgeSwapAlgo = TypeParam;

       EdgeVector edge_list;
       edge_list.push_back({0, 2});
       edge_list.push_back({1, 2});
       edge_list.push_back({2, 3});
       edge_list.push_back({4, 5});

       SwapVector swap_list;

       swap_list.push_back({2, 3, true});
       swap_list.push_back({0, 1, true});

       EdgeSwapAlgo algo(edge_list, swap_list);
       algo.setDisplayDebug(debug_this_test);

       if (EdgeSwapTrait<EdgeSwapAlgo>::pushableSwaps()) {
         for (auto &s : swap_list)
            algo.push(s);
       }
       algo.run();

       this->_print_list(edge_list, debug_this_test);

       auto & debug = algo.debugVector();
       this->_print_list(debug, debug_this_test);

       ASSERT_TRUE(debug[0].performed);
       ASSERT_FALSE(debug[1].performed);
       ASSERT_TRUE(debug[1].conflictDetected[0]);
       ASSERT_TRUE(debug[1].conflictDetected[1]);
   }
}
#else
    class TestEdgeSwap : public ::testing::Test {};
    TEST_F(TestEdgeSwap, warning) {
        EXPECT_TRUE(false) << "TestEdgeSwap Tests are disabled since built without -DEDGE_SWAP_DEBUG_VECTOR";
    }
#endif