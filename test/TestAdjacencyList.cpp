/**
 * @file TestAdjacencyList.cpp
 *
 * @date 2. October 2017
 *
 * @author Hung Tran
 */
#include <gtest/gtest.h>

#include "defs.h"
#include "Curveball/IMAdjacencyList.h"

class TestAdjacencyList : public ::testing::Test {
};

using degree_vector = std::vector<degree_t>;
using neighbour_it = std::vector<node_t>::const_iterator;

TEST_F(TestAdjacencyList, constructor) {
	degree_vector degrees;
	degrees.push_back(3);
	degrees.push_back(2);
	degrees.push_back(1);
	degrees.push_back(1);
	degrees.push_back(1);

	const edgeid_t degree_sum = 8;

	Curveball::IMAdjacencyList adj_list(5, degree_sum);
	adj_list.initialize(degrees, {10, 11, 12, 13, 14}, 5, degree_sum);

	// Test beginning iterator for 0
	neighbour_it n_it = adj_list.cbegin(0);
	++n_it;
	++n_it;
	++n_it;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;

	n_it++;
	n_it++;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;

	n_it++;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;

	// Test beginning iterator for 1
	n_it = adj_list.cbegin(1);
	++n_it;
	++n_it;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);

	// Test ending iterator for 2
	n_it = adj_list.cbegin(2);
	neighbour_it e_it = adj_list.cend(2);
	ASSERT_EQ(n_it, e_it);
}

TEST_F(TestAdjacencyList, initialize_method) {
	degree_vector degrees;
	degrees.push_back(3);
	degrees.push_back(2);
	degrees.push_back(1);
	degrees.push_back(1);
	degrees.push_back(1);

	const edgeid_t degree_sum = 8;

	Curveball::IMAdjacencyList adj_list(5, degree_sum);
	adj_list.initialize(degrees, {10, 11, 12, 13, 14}, 5, degree_sum);

	// Test beginning iterator for 0
	neighbour_it n_it = adj_list.cbegin(0);
	++n_it;
	++n_it;
	++n_it;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;

	n_it++;
	n_it++;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;

	n_it++;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;

	// Test beginning iterator for 1
	n_it = adj_list.cbegin(1);
	++n_it;
	++n_it;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);

	// Test ending iterator for 2
	n_it = adj_list.cbegin(2);
	neighbour_it e_it = adj_list.cend(2);
	ASSERT_EQ(n_it, e_it);
}

TEST_F(TestAdjacencyList, insertion) {
	degree_vector degrees;
	degrees.push_back(3);
	degrees.push_back(2);
	degrees.push_back(1);
	degrees.push_back(1);
	degrees.push_back(1);

	const edgeid_t degree_sum = 8;

	Curveball::IMAdjacencyList adj_list(5, degree_sum);
	adj_list.initialize(degrees, {10, 11, 12, 13, 14}, 5, degree_sum);

	adj_list.insert_neighbour(0, 3);
	adj_list.insert_neighbour(0, 2);
	neighbour_it n_it = adj_list.cbegin(0);
	ASSERT_EQ(*n_it, 3);
	n_it++;
	ASSERT_EQ(*n_it, 2);
	n_it++;
	ASSERT_EQ(n_it, adj_list.cend(0));
}

TEST_F(TestAdjacencyList, reset) {
	degree_vector degrees;
	degrees.push_back(3);
	degrees.push_back(2);
	degrees.push_back(1);
	degrees.push_back(1);
	degrees.push_back(1);

	const edgeid_t degree_sum = 8;

	Curveball::IMAdjacencyList adj_list(5, degree_sum);
	adj_list.initialize(degrees, {10, 11, 12, 13, 14}, 5, degree_sum);
	adj_list.insert_neighbour(0, 3ul);
	adj_list.insert_neighbour(0, 2ul);
	neighbour_it n_it = adj_list.cbegin(0);
	ASSERT_EQ(*n_it, 3);
	n_it++;
	ASSERT_EQ(*n_it, 2);
	n_it++;
	ASSERT_EQ(n_it, adj_list.cend(0));

	adj_list.set_traded(0);
	adj_list.reset_row(0);
	ASSERT_EQ(adj_list.cbegin(0), adj_list.cend(0));

	adj_list.insert_neighbour(0, 1);
	n_it = adj_list.cbegin(0);
	ASSERT_EQ(*n_it, 1);
	n_it++;
	ASSERT_EQ(n_it, adj_list.cend(0));

}

TEST_F(TestAdjacencyList, parallel_insertion) {
	const int NUM_THREADS = omp_get_max_threads();

	degree_vector degrees;
	degrees.push_back(NUM_THREADS * 100);
	degrees.push_back(NUM_THREADS * 100);
	degrees.push_back(NUM_THREADS * 100);
	degrees.push_back(NUM_THREADS * 100);

	const edgeid_t degree_sum = NUM_THREADS * 400;

	Curveball::IMAdjacencyList adj_list(4, degree_sum);
	adj_list.initialize(degrees, {6, 7, 8, 9}, 4, degree_sum);

	for (node_t i = 0; i < 4; i++)
		ASSERT_EQ(adj_list.degree_at(i), NUM_THREADS * 100);

	for (size_t i = 0; i < 100; i++) {
		#pragma omp parallel for num_threads(NUM_THREADS)
		for (node_t j = 0; j < NUM_THREADS; j++) {
			for (node_t k = 0; k < 4; k++) {
				adj_list.insert_neighbour_without_check(k, static_cast<node_t>(j));
			}
		}

		for (node_t k = 0; k < 4; k++)
			ASSERT_EQ(adj_list.received_msgs(k), (i+1)*NUM_THREADS);
	}

	EXPECT_EQ(adj_list.received_msgs(0), NUM_THREADS * 100);
	EXPECT_EQ(adj_list.received_msgs(1), NUM_THREADS * 100);
	EXPECT_EQ(adj_list.received_msgs(2), NUM_THREADS * 100);
	EXPECT_EQ(adj_list.received_msgs(3), NUM_THREADS * 100);
}

TEST_F(TestAdjacencyList, organize_neighbours_sort) {
	degree_vector degrees;
	degrees.push_back(5);
	degrees.push_back(5);

	const edgeid_t degree_sum = 10;

	Curveball::IMAdjacencyList adj_list(2, degree_sum);
	adj_list.initialize(degrees, {6, 7}, 2, degree_sum);

	adj_list.set_traded(0);
	adj_list.insert_neighbour(0, 1);
	adj_list.insert_neighbour(0, Curveball::LISTROW_END);
	adj_list.insert_neighbour(0, 3);
	adj_list.insert_neighbour(0, 4);
	adj_list.insert_neighbour(0, 2);

	neighbour_it n_it = adj_list.begin(0);
	ASSERT_EQ(*n_it, 1);
	n_it++;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;
	ASSERT_EQ(*n_it, 3);
	n_it++;
	ASSERT_EQ(*n_it, 4);
	n_it++;
	ASSERT_EQ(*n_it, 2);
	n_it++;
	ASSERT_EQ(*n_it, Curveball::IS_TRADED);

	// the row looks like this
	// | 1, LISTROW_END, 3, 4, 2, IS_TRADED | 0, 0, 0, 0, 0, LISTROWEND |
	// when sorting begin(0) to end(0) we expect
	// | 1, 2, 3, 4, LISTROW_END, IS_TRADED | 0, 0, 0, 0, 0, LISTROWEND |
	// that the sentinel is not considered when sorting
	std::sort(adj_list.begin(0), adj_list.end(0));

	n_it = adj_list.begin(0);
	ASSERT_EQ(*n_it, 1);
	n_it++;
	ASSERT_EQ(*n_it, 2);
	n_it++;
	ASSERT_EQ(*n_it, 3);
	n_it++;
	ASSERT_EQ(*n_it, 4);
	n_it++;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;
	ASSERT_EQ(*n_it, Curveball::IS_TRADED);

	// we expect that end(0) is evaluated to IS_TRADED and not LISTROW_END
	ASSERT_EQ(*adj_list.end(0), Curveball::IS_TRADED);
	ASSERT_EQ(*(adj_list.end(0) - 1), Curveball::LISTROW_END);
}

TEST_F(TestAdjacencyList, organize_neighbours_execution) {
	degree_vector degrees;
	degrees.push_back(5);
	degrees.push_back(5);

	const edgeid_t degree_sum = 10;

	Curveball::IMAdjacencyList adj_list(2, degree_sum);
	adj_list.initialize(degrees, {6, 7}, 2, degree_sum);

	adj_list.set_traded(0);
	adj_list.insert_neighbour(0, 1);
	adj_list.insert_neighbour(0, Curveball::LISTROW_END);
	adj_list.insert_neighbour(0, 3);
	adj_list.insert_neighbour(0, 4);
	adj_list.insert_neighbour(0, 2);

	auto organize_neighbours = [&](const node_t mc_node_u, const node_t v) {
	  auto pos =
		  std::find
			  (adj_list.begin(mc_node_u), adj_list.end(mc_node_u), v);
	  if (pos == adj_list.cend(mc_node_u)) {
		  // not found, sort u's row
		  std::sort(adj_list.begin(mc_node_u), adj_list.end(mc_node_u));
	  } else {
		  // v is found in u's adjacency structure
		  // put MAXINT into v's position
		  *pos = Curveball::LISTROW_END;
		  // then sort
		  std::sort(adj_list.begin(mc_node_u), adj_list.end(mc_node_u));
		  // and replace MAXINT with v again, such that
		  // is at last position in the row
		  *(adj_list.end(mc_node_u) - 1) = v;
	  }
	};

	organize_neighbours(0, 1);
	// 1, LISTROW_END, 3, 4, 2, IS_TRADED
	// => 2, 3, 4, LISTROW_END, 1, IS_TRADED
	auto n_it = adj_list.begin(0);
	ASSERT_EQ(*n_it, 2);
	n_it++;
	ASSERT_EQ(*n_it, 3);
	n_it++;
	ASSERT_EQ(*n_it, 4);
	n_it++;
	ASSERT_EQ(*n_it, Curveball::LISTROW_END);
	n_it++;
	ASSERT_EQ(*n_it, 1);
	n_it++;
	ASSERT_EQ(*n_it, Curveball::IS_TRADED);
}

TEST_F(TestAdjacencyList, tradable1) {
	degree_vector degrees;
	degrees.push_back(400);
	degrees.push_back(400);

	// 1 not in 0, but 0 in 1's row

	const edgeid_t degree_sum = 800;

	Curveball::IMAdjacencyList adj_list(2, degree_sum);
	// trading nodes share an edge
	adj_list.initialize(degrees, {1, 0}, 2, degree_sum);

	for (int i = 0; i < 399; i++)
		adj_list.insert_neighbour_at(0, i, i);

	for (int i = 0; i < 399; i++)
		adj_list.insert_neighbour_at(1, i, i);

	adj_list.set_offset(0, 399);
	adj_list.set_offset(1, 399);

	ASSERT_TRUE(adj_list.get_edge_in_partner(0));
	ASSERT_FALSE(adj_list.tradable(0, 1));

	adj_list.insert_neighbour_at(0, 399, 399);
	adj_list.set_offset(0, 400);
	ASSERT_TRUE(adj_list.tradable(0, 1));
}


TEST_F(TestAdjacencyList, tradable2) {
	degree_vector degrees;
	degrees.push_back(400);
	degrees.push_back(400);

	// 1 not in 0, but 0 in 1's row

	const edgeid_t degree_sum = 800;

	Curveball::IMAdjacencyList adj_list(2, degree_sum);
	// trading nodes do not share an edge
	adj_list.initialize(degrees, {1000, 0}, 2, degree_sum);

	for (int i = 0; i < 399; i++)
		adj_list.insert_neighbour_at(0, i, i);

	for (int i = 0; i < 400; i++)
		adj_list.insert_neighbour_at(1, i, i);

	adj_list.set_offset(0, 399);
	adj_list.set_offset(1, 400);

	ASSERT_FALSE(adj_list.get_edge_in_partner(0));
	ASSERT_FALSE(adj_list.tradable(0, 1));

	adj_list.insert_neighbour_at(0, 399, 399);
	adj_list.set_offset(0, 400);
	ASSERT_TRUE(adj_list.tradable(0, 1));
}