#include <gtest/gtest.h>

#include <stxxl/vector>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/EdgeSwapTFP.h>
#include <EdgeSwaps/MultiEdgeSwapFullyInternal.h>
#include <EdgeSwaps/EdgeSwapParallelTFP.h>
#include <EdgeSwaps/IMEdgeSwap.h>

class TestNewFullyInternal : public ::testing::Test {
};

using EdgeVector = stxxl::vector<edge_t>;
using SwapVector = stxxl::vector<SwapDescriptor>;
using Algo = MultiEdgeSwapFullyInternal<>;
TEST_F(TestNewFullyInternal, test2) {

   		EdgeVector edge_list;
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 9});
   		edge_list.push_back({2, 10});
   		edge_list.push_back({3, 4});
   		edge_list.push_back({5, 6});
   		edge_list.push_back({7, 8});
	
		SwapVector swap_list;
   		swap_list.push_back({0, 5, false});
   		swap_list.push_back({1, 6, false});
   		swap_list.push_back({2, 7, false});
   		swap_list.push_back({3, 4, false});

	    Algo esfi(edge_list, swap_list);
		esfi.run();

		auto edge_list_ = esfi.new_edges();

		for (unsigned int i = 0; i < edge_list_.size(); ++i) {
			std::cout << edge_list_[i] << std::endl;
		}

		auto debug_list_ = esfi.debugVector();

		for (unsigned int i = 0; i < debug_list_.size(); ++i) {
			std::cout << debug_list_[i] << std::endl;
		}

	    ASSERT_EQ(edge_list_[0], edge_t(1, 2));
	    ASSERT_EQ(edge_list_[1], edge_t(1, 3));
	    ASSERT_EQ(edge_list_[2], edge_t(1, 5));
	    ASSERT_EQ(edge_list_[3], edge_t(1, 7));
	    ASSERT_EQ(edge_list_[4], edge_t(2, 4));
	    ASSERT_EQ(edge_list_[5], edge_t(2, 6));
	    ASSERT_EQ(edge_list_[6], edge_t(2, 8));
	    ASSERT_EQ(edge_list_[7], edge_t(9, 10));

}

TEST_F(TestNewFullyInternal, deletion) {

   		EdgeVector edge_list;
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 3});
   		edge_list.push_back({2, 4});
   		edge_list.push_back({5, 6});
	
		SwapVector swap_list;
   		swap_list.push_back({0, 5, true});
   		swap_list.push_back({3, 4, false});

	    Algo esfi(edge_list, swap_list);
		esfi.run();

		auto edge_list_ = esfi.new_edges();

		for (unsigned int i = 0; i < edge_list_.size(); ++i) {
			std::cout << edge_list_[i] << std::endl;
		}

		auto debug_list_ = esfi.debugVector();

		for (unsigned int i = 0; i < debug_list_.size(); ++i) {
			std::cout << debug_list_[i] << std::endl;
		}

	    ASSERT_EQ(edge_list_[0], edge_t(1, 2));
	    ASSERT_EQ(edge_list_[1], edge_t(1, 2));
	    ASSERT_EQ(edge_list_[2], edge_t(1, 3));
	    ASSERT_EQ(edge_list_[3], edge_t(1, 6));
	    ASSERT_EQ(edge_list_[4], edge_t(2, 4));
	    ASSERT_EQ(edge_list_[5], edge_t(2, 5));

}

TEST_F(TestNewFullyInternal, fullyInternalGuaranteed) {

	EdgeVector edge_list;
	edge_list.push_back({1, 3});
	edge_list.push_back({2, 4});
	edge_list.push_back({2, 4});
	edge_list.push_back({3, 3});
	edge_list.push_back({3, 6});
	edge_list.push_back({5, 6});

	SwapVector swap_list;
	swap_list.push_back({0, 1, true});
	swap_list.push_back({1, 2, false});
	swap_list.push_back({3, 5, true});

	Algo esfi(edge_list, swap_list);
	esfi.run();

	auto edge_list_ = esfi.new_edges();

	for (unsigned int i = 0; i < edge_list_.size(); ++i) {
		std::cout << edge_list_[i] << std::endl;
	}

	auto debug_list_ = esfi.debugVector();

	for (unsigned int i = 0; i < debug_list_.size(); ++i) {
		std::cout << debug_list_[i] << std::endl;
	}

	ASSERT_EQ(edge_list_[0], edge_t(1, 4));
    ASSERT_EQ(edge_list_[1], edge_t(2, 3));
    ASSERT_EQ(edge_list_[2], edge_t(2, 4));
    ASSERT_EQ(edge_list_[3], edge_t(3, 3));
    ASSERT_EQ(edge_list_[4], edge_t(3, 6));
    ASSERT_EQ(edge_list_[5], edge_t(5, 6));
}