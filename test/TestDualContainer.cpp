/**
 * @file TestDualContainer.cpp
 * @date 29. September 2017
 *
 * @author Hung Tran
 */
#include <gtest/gtest.h>

#include "Utils/NodeHash.h"
#include "Curveball/EMDualContainer.h"

class TestDualContainer : public ::testing::Test {
};

TEST_F(TestDualContainer, vector_sizes) {
	for (Curveball::chunkid_t num_chunks = 2; num_chunks <= 8; num_chunks++) {
		for (node_t num_nodes = 100; num_nodes < 100000; num_nodes++) {
			const node_t nodes_per_mc = Curveball::make_even_by_sub(
				static_cast<node_t>(num_nodes / num_chunks));
			const node_t last_mc_size = num_nodes - (num_chunks - 1) * nodes_per_mc;

			ASSERT_EQ(last_mc_size, Curveball::get_last_mc_nodes(num_nodes, num_chunks));
		}
	}
}