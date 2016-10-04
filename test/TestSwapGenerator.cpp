#include <gtest/gtest.h>
#include "SwapGenerator.h"

class TestSwapGenerator : public ::testing::Test {};

TEST_F(TestSwapGenerator, count) {
   for(int64_t num = 0; num < 100; num++) {
      const int64_t edges = 10;
      SwapGenerator gen(num, edges, 1234*num);

      for(int64_t i=0; i < num; i++) {
         ASSERT_FALSE(gen.empty());
         const auto & swap = *gen;
         ASSERT_LT(swap.edges()[0], edges);
         ASSERT_LT(swap.edges()[1], edges);
         ASSERT_LT(swap.edges()[0], swap.edges()[1]);
         ++gen;
      }

      ASSERT_TRUE(gen.empty());
   }
}
