/**
 * @file TestMacrochunk.cpp
 * @date 27. September 2017
 *
 * @author Hung Tran
 */
#include <gtest/gtest.h>

#include "defs.h"
#include "Curveball/IMMacrochunk.h"

class TestMacrochunk : public ::testing::Test {
};

TEST_F(TestMacrochunk, move_constructor) {
	std::vector<Curveball::IMMacrochunk<>> macrochunks;
	macrochunks.reserve(10);

	for (unsigned int i = 0; i < 10; i++) {
		macrochunks.emplace_back(i, i);
	}

	for (unsigned int i = 0; i < 10; i++) {
		ASSERT_EQ(macrochunks[i].get_chunkid(), i);
		ASSERT_EQ(macrochunks[i].get_msg_count(), 0ul);
	}
}

TEST_F(TestMacrochunk, insertion) {
	Curveball::IMMacrochunk<> msg_chunk(0, 200000);

	for (unsigned long i = 0; i < 100000; i++) {
		msg_chunk.push_sequential(Curveball::NeighbourMsg{0, 0});
	}

	Curveball::msg_vector loaded_msgs;
	const bool loaded_all = msg_chunk.load_messages(loaded_msgs);

	ASSERT_TRUE(loaded_all);

	for (auto const_it = loaded_msgs.cbegin();
		 const_it != loaded_msgs.cend();
		 const_it++) {
		ASSERT_EQ((*const_it).neighbour, 0ul);
		ASSERT_EQ((*const_it).target, 0ul);
	}
}