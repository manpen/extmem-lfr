/**
 * @file TestMessageContainer.cpp
 * @date 28. September 2017
 *
 * @author Hung Tran
 */
#include <gtest/gtest.h>
#include <EdgeStream.h>
#include <Utils/NodeHash.h>
#include <Curveball/EMTargetInformation.h>

#include "Curveball/EMMessageContainer.h"

class TestMessageContainer : public ::testing::Test {
};

using hnode_t = node_t;
using NeighbourMsg = Curveball::NeighbourMsg;
using TargetMsg = Curveball::TargetMsg;
using msg_vector = Curveball::msg_vector;
using ContainerType = Curveball::EMMessageContainer<Curveball::ModHash>;

TEST_F(TestMessageContainer, parallel_insertion) {
	std::vector<hnode_t> upper_bounds = {100, 200, 300, 400};
	ContainerType buffers(upper_bounds,
						  Curveball::DUMMY_LIMIT,
						  ContainerType::ACTIVE,
						  Curveball::DUMMY_THREAD_NUM,
						  1000);

	#pragma omp parallel for num_threads(Curveball::DUMMY_THREAD_NUM)
	for (uint_t i = 0; i < Curveball::DUMMY_THREAD_NUM * 100000; i++) {
		buffers.push(NeighbourMsg{0, static_cast<node_t>(omp_get_thread_num())},
					 omp_get_thread_num());
		buffers.push(NeighbourMsg{100, static_cast<node_t>(omp_get_thread_num())},
					 omp_get_thread_num());
		buffers.push(NeighbourMsg{200, static_cast<node_t>(omp_get_thread_num())},
					 omp_get_thread_num());
		buffers.push(NeighbourMsg{300, static_cast<node_t>(omp_get_thread_num())},
					 omp_get_thread_num());
	}

	for (uint i = 0; i < Curveball::DUMMY_THREAD_NUM; i++)
		buffers.force_push(i);

	std::vector<degree_t> thread(Curveball::DUMMY_THREAD_NUM, 0);
	for (Curveball::chunkid_t i = 0; i < Curveball::DUMMY_THREAD_NUM; i++) {
		const auto msgs = buffers.get_messages_of(i);
		for (const auto msg : msgs) {
			thread[msg.neighbour]++;
		}
	}

	for (size_t i = 0; i < Curveball::DUMMY_THREAD_NUM; i++)
		ASSERT_EQ(thread[i], Curveball::DUMMY_THREAD_NUM * 100000ul);
}

TEST_F(TestMessageContainer, initialization) {
	std::vector<hnode_t> upper_bounds = {1, 5, 10};
	ContainerType buffers(upper_bounds, 4,
						  ContainerType::ACTIVE,
						  Curveball::DUMMY_THREAD_NUM, 1000);
}

TEST_F(TestMessageContainer, chunk_targetting) {
	std::vector<hnode_t> upper_bounds = {100, 200, 300};
	ContainerType buffers(upper_bounds, 100,
						  ContainerType::ACTIVE,
						  Curveball::DUMMY_THREAD_NUM, 1000);

	// we have 3 chunks,
	// chunk0:   0 -  99
	// chunk1: 100 - 199
	// chunk2: 200 - 299
	ASSERT_EQ(buffers.get_target_chunkid(0), 0u);
	ASSERT_EQ(buffers.get_target_chunkid(99), 0u);
	ASSERT_EQ(buffers.get_target_chunkid(100), 1u);
	ASSERT_EQ(buffers.get_target_chunkid(199), 1u);
	ASSERT_EQ(buffers.get_target_chunkid(200), 2u);
	ASSERT_EQ(buffers.get_target_chunkid(299), 2u);
}

TEST_F(TestMessageContainer, insertion) {
	std::vector<hnode_t> upper_bounds = {100, 200, 300};
	ContainerType buffers(upper_bounds, 100,
						  ContainerType::ACTIVE,
						  Curveball::DUMMY_THREAD_NUM, 1000);

	ASSERT_EQ(buffers.get_size_of_chunk(0), 0ul);
	ASSERT_EQ(buffers.get_size_of_chunk(1), 0ul);
	ASSERT_EQ(buffers.get_size_of_chunk(2), 0ul);
	buffers.push(NeighbourMsg{0, 0});
	buffers.push(NeighbourMsg{99, 1});
	ASSERT_EQ(buffers.get_messages_of(0).size(), 2ul);
	buffers.push(NeighbourMsg{100, 2});
	buffers.push(NeighbourMsg{199, 3});
	ASSERT_EQ(buffers.get_messages_of(1).size(), 2ul);
	buffers.push(NeighbourMsg{200, 4});
	buffers.push(NeighbourMsg{299, 5});
	ASSERT_EQ(buffers.get_messages_of(2).size(), 2ul);
}

TEST_F(TestMessageContainer, full_insertion) {
	std::vector<hnode_t> upper_bounds = {100, 200, 300};
	ContainerType buffers(upper_bounds, 100,
						  ContainerType::ACTIVE,
						  Curveball::DUMMY_THREAD_NUM, 1000);

	for (hnode_t i = 0; i < 100; i++) {
		buffers.push(NeighbourMsg{i, i});
	}

	ASSERT_EQ(buffers.get_messages_of(0).size(), 100u);

	buffers.get_messages_of(0);
}

TEST_F(TestMessageContainer, load_messages) {
	std::vector<hnode_t> upper_bounds = {100, 200, 300};
	ContainerType buffers(upper_bounds, 100,
						  ContainerType::ACTIVE,
						  Curveball::DUMMY_THREAD_NUM, 1000);

	msg_vector expected_msgs0 = {NeighbourMsg{0, 0}, NeighbourMsg{10, 1},
								 NeighbourMsg{99, 2}, NeighbourMsg{33, 6}};
	msg_vector expected_msgs1 = {NeighbourMsg{100, 3}, NeighbourMsg{115, 4}};
	msg_vector expected_msgs2 = {NeighbourMsg{223, 5}, NeighbourMsg{277, 7}};
	std::sort(expected_msgs0.begin(), expected_msgs0.end());
	std::sort(expected_msgs1.begin(), expected_msgs1.end());
	std::sort(expected_msgs2.begin(), expected_msgs2.end());

	buffers.push(NeighbourMsg{223, 5});
	buffers.push(NeighbourMsg{115, 4});
	buffers.push(NeighbourMsg{100, 3});
	buffers.push(NeighbourMsg{99, 2});
	buffers.push(NeighbourMsg{10, 1});
	buffers.push(NeighbourMsg{0, 0});
	buffers.push(NeighbourMsg{33, 6});
	buffers.push(NeighbourMsg{277, 7});

	msg_vector loaded_msg0 = buffers.get_messages_of(0);
	msg_vector loaded_msg1 = buffers.get_messages_of(1);
	msg_vector loaded_msg2 = buffers.get_messages_of(2);
	std::sort(loaded_msg0.begin(), loaded_msg0.end());
	std::sort(loaded_msg1.begin(), loaded_msg1.end());
	std::sort(loaded_msg2.begin(), loaded_msg2.end());

	ASSERT_EQ(loaded_msg0, expected_msgs0);
	ASSERT_EQ(loaded_msg1, expected_msgs1);
	ASSERT_EQ(loaded_msg2, expected_msgs2);
}

TEST_F(TestMessageContainer, load_messages2) {
	std::vector<degree_t> degrees(131);

	EdgeStream edges;
	for (node_t i = 0; i < 111; i++)
		for (node_t j = i + 40; j < 131; j++) {
			edges.push(edge_t{i, j});
			degrees[i]++;
			degrees[j]++;
		}

	Curveball::ModHash hash_func = Curveball::ModHash::get_random(131);

	Curveball::EMTargetInformation deg_helper(4, 131);

	for (node_t node = 0; node < 131; node++) {
		deg_helper.push_active(TargetMsg{hash_func.hash(node), degrees[node], 0});
	}

	deg_helper.sort();

	ContainerType msgs_container(deg_helper.get_bounds_active(),
								 Curveball::DUMMY_LIMIT,
								 ContainerType::ACTIVE,
								 Curveball::DUMMY_THREAD_NUM,
								 1000);

	edges.rewind();

	for (; !edges.empty(); ++edges) {
		const edge_t edge = *edges;

		const hnode_t h_fst = hash_func.hash(edge.first);
		const hnode_t h_snd = hash_func.hash(edge.second);

		if (h_fst < h_snd)
			msgs_container.push(NeighbourMsg{h_fst, edge.second});
		else
			msgs_container.push(NeighbourMsg{h_snd, edge.first});
	}

	edges.rewind();

	ASSERT_EQ(edges.size(), msgs_container.get_size_of_chunk(0)
							+ msgs_container.get_size_of_chunk(1)
							+ msgs_container.get_size_of_chunk(2)
							+ msgs_container.get_size_of_chunk(3));
}

TEST_F(TestMessageContainer, reset) {
	std::vector<hnode_t> upper_bounds = {100, 200, 300};
	ContainerType buffers(upper_bounds,
						  100,
						  ContainerType::ACTIVE,
						  Curveball::DUMMY_THREAD_NUM,
						  1000);

	buffers.push(NeighbourMsg{223, 5});
	buffers.push(NeighbourMsg{115, 4});
	buffers.push(NeighbourMsg{100, 3});
	buffers.push(NeighbourMsg{99, 2});
	buffers.push(NeighbourMsg{10, 1});
	buffers.push(NeighbourMsg{0, 0});
	buffers.push(NeighbourMsg{33, 6});
	buffers.push(NeighbourMsg{277, 7});

	ASSERT_EQ(buffers.get_messages_of(0).size(), 4ul);
	ASSERT_EQ(buffers.get_messages_of(1).size(), 2ul);
	ASSERT_EQ(buffers.get_messages_of(2).size(), 2ul);

	buffers.reset();

	buffers.push(NeighbourMsg{223, 5});
	buffers.push(NeighbourMsg{115, 4});
	buffers.push(NeighbourMsg{100, 3});
	buffers.push(NeighbourMsg{99, 2});
	buffers.push(NeighbourMsg{10, 1});
	buffers.push(NeighbourMsg{0, 0});
	buffers.push(NeighbourMsg{33, 6});
	buffers.push(NeighbourMsg{277, 7});
	buffers.push(NeighbourMsg{224, 5});
	buffers.push(NeighbourMsg{116, 4});
	buffers.push(NeighbourMsg{101, 3});
	buffers.push(NeighbourMsg{98, 2});
	buffers.push(NeighbourMsg{11, 1});
	buffers.push(NeighbourMsg{1, 0});
	buffers.push(NeighbourMsg{34, 6});
	buffers.push(NeighbourMsg{278, 7});

	ASSERT_EQ(buffers.get_messages_of(0).size(), 8ul);
	ASSERT_EQ(buffers.get_messages_of(1).size(), 4ul);
	ASSERT_EQ(buffers.get_messages_of(2).size(), 4ul);
}

TEST_F(TestMessageContainer, swap) {
	std::vector<hnode_t> upper_bounds = {100, 200, 300};
	ContainerType buffers(upper_bounds,
						  100,
						  ContainerType::ACTIVE,
						  Curveball::DUMMY_THREAD_NUM,
						  1000);
	ContainerType other(upper_bounds,
						100,
						ContainerType::PENDING,
						Curveball::DUMMY_THREAD_NUM,
						1000);

	// fill original
	buffers.push(NeighbourMsg{99, 2});
	buffers.push(NeighbourMsg{10, 1});
	buffers.push(NeighbourMsg{0, 0});
	buffers.push(NeighbourMsg{33, 6});

	buffers.push(NeighbourMsg{115, 4});
	buffers.push(NeighbourMsg{100, 3});

	buffers.push(NeighbourMsg{223, 5});
	buffers.push(NeighbourMsg{277, 7});

	// fill other
	other.push(NeighbourMsg{23, 5});
	other.push(NeighbourMsg{77, 7});

	other.push(NeighbourMsg{199, 2});
	other.push(NeighbourMsg{110, 1});
	other.push(NeighbourMsg{100, 0});
	other.push(NeighbourMsg{133, 6});

	other.push(NeighbourMsg{215, 4});
	other.push(NeighbourMsg{200, 3});
	other.push(NeighbourMsg{216, 4});
	other.push(NeighbourMsg{207, 3});

	ASSERT_EQ(buffers.get_messages_of(0).size(), 4ul);
	ASSERT_EQ(buffers.get_messages_of(1).size(), 2ul);
	ASSERT_EQ(buffers.get_messages_of(2).size(), 2ul);
	ASSERT_EQ(other.get_messages_of(0).size(), 2ul);
	ASSERT_EQ(other.get_messages_of(1).size(), 4ul);
	ASSERT_EQ(other.get_messages_of(2).size(), 4ul);

	buffers.swap_with_next(other);

	ASSERT_EQ(buffers.get_messages_of(0).size(), 2ul);
	ASSERT_EQ(buffers.get_messages_of(1).size(), 4ul);
	ASSERT_EQ(buffers.get_messages_of(2).size(), 4ul);

	ASSERT_FALSE(buffers.empty());
	ASSERT_TRUE(other.empty());
}