/**
 * @file TestHashing.cpp
 * @date 30. September 2017
 *
 * @author Hung Tran
 */
#include <gtest/gtest.h>

#include "defs.h"
#include "Utils/NodeHash.h"

class TestHashing : public ::testing::Test {
};

TEST_F(TestHashing, inverse_in_Z3) {
	ASSERT_EQ(Curveball::inverse_of_in(1, 3), 1ul);
	ASSERT_EQ(Curveball::inverse_of_in(2, 3), 2ul);
}

TEST_F(TestHashing, inverse_in_Z5) {
	ASSERT_EQ(Curveball::inverse_of_in(1, 5), 1ul);
	ASSERT_EQ(Curveball::inverse_of_in(2, 5), 3ul);
	ASSERT_EQ(Curveball::inverse_of_in(3, 5), 2ul);
	ASSERT_EQ(Curveball::inverse_of_in(4, 5), 4ul);
}

TEST_F(TestHashing, inverse_in_Z7) {
	ASSERT_EQ(Curveball::inverse_of_in(1, 7), 1ul);
	ASSERT_EQ(Curveball::inverse_of_in(2, 7), 4ul);
	ASSERT_EQ(Curveball::inverse_of_in(3, 7), 5ul);
	ASSERT_EQ(Curveball::inverse_of_in(4, 7), 2ul);
	ASSERT_EQ(Curveball::inverse_of_in(5, 7), 3ul);
	ASSERT_EQ(Curveball::inverse_of_in(6, 7), 6ul);
}

TEST_F(TestHashing, inverse_in_Z11) {
	ASSERT_EQ(Curveball::inverse_of_in(1, 11), 1ul);
	ASSERT_EQ(Curveball::inverse_of_in(2, 11), 6ul);
	ASSERT_EQ(Curveball::inverse_of_in(3, 11), 4ul);
	ASSERT_EQ(Curveball::inverse_of_in(4, 11), 3ul);
	ASSERT_EQ(Curveball::inverse_of_in(5, 11), 9ul);
	ASSERT_EQ(Curveball::inverse_of_in(6, 11), 2ul);
	ASSERT_EQ(Curveball::inverse_of_in(7, 11), 8ul);
	ASSERT_EQ(Curveball::inverse_of_in(8, 11), 7ul);
	ASSERT_EQ(Curveball::inverse_of_in(9, 11), 5ul);
	ASSERT_EQ(Curveball::inverse_of_in(10, 11), 10ul);
}

TEST_F(TestHashing, inverse_in_Z13) {
	ASSERT_EQ(Curveball::inverse_of_in(1, 13), 1ul);
	ASSERT_EQ(Curveball::inverse_of_in(2, 13), 7ul);
	ASSERT_EQ(Curveball::inverse_of_in(3, 13), 9ul);
	ASSERT_EQ(Curveball::inverse_of_in(4, 13), 10ul);
	ASSERT_EQ(Curveball::inverse_of_in(5, 13), 8ul);
	ASSERT_EQ(Curveball::inverse_of_in(6, 13), 11ul);
	ASSERT_EQ(Curveball::inverse_of_in(7, 13), 2ul);
	ASSERT_EQ(Curveball::inverse_of_in(8, 13), 5ul);
	ASSERT_EQ(Curveball::inverse_of_in(9, 13), 3ul);
	ASSERT_EQ(Curveball::inverse_of_in(10, 13), 4ul);
	ASSERT_EQ(Curveball::inverse_of_in(11, 13), 6ul);
	ASSERT_EQ(Curveball::inverse_of_in(12, 13), 12ul);
}

TEST_F(TestHashing, sentinels_modhash_Z11) {
	Curveball::ModHash h(2, 3, 11);

	ASSERT_EQ(h.hash(h.min_value()), 0ul);
	ASSERT_EQ(h.hash(h.max_value()), 10ul);
}

TEST_F(TestHashing, sentinels_modhash_Z31) {
	Curveball::ModHash h(2, 3, 31);

	ASSERT_EQ(h.hash(h.min_value()), 0ul);
	ASSERT_EQ(h.hash(h.max_value()), 30ul);
}

TEST_F(TestHashing, sentinels_modhash_Z41) {
	Curveball::ModHash h(2, 3, 41);

	ASSERT_EQ(h.hash(h.min_value()), 0ul);
	ASSERT_EQ(h.hash(h.max_value()), 40ul);
}

TEST_F(TestHashing, get_identity_modhash) {
	Curveball::ModHash id = Curveball::ModHash::get_identity(10000);

	for (uint_t i = 0; i < 10000; i++) {
		ASSERT_EQ(i, id.hash(i));
	}
}

TEST_F(TestHashing, next_prime) {
	ASSERT_EQ(Curveball::get_next_prime(8), 11ul);
	ASSERT_EQ(Curveball::get_next_prime(9), 11ul);
	ASSERT_EQ(Curveball::get_next_prime(10), 11ul);
	ASSERT_EQ(Curveball::get_next_prime(11), 13ul);
	ASSERT_EQ(Curveball::get_next_prime(12), 13ul);
	ASSERT_EQ(Curveball::get_next_prime(13), 17ul);
	ASSERT_EQ(Curveball::get_next_prime(14), 17ul);
	ASSERT_EQ(Curveball::get_next_prime(17), 19ul);
	ASSERT_EQ(Curveball::get_next_prime(18), 19ul);
	ASSERT_EQ(Curveball::get_next_prime(20), 23ul);
	ASSERT_EQ(Curveball::get_next_prime(24), 29ul);
	ASSERT_EQ(Curveball::get_next_prime(28), 29ul);
	ASSERT_EQ(Curveball::get_next_prime(32), 37ul);
	ASSERT_EQ(Curveball::get_next_prime(38), 41ul);
	ASSERT_EQ(Curveball::get_next_prime(42), 43ul);
	ASSERT_EQ(Curveball::get_next_prime(100), 101ul);
	ASSERT_EQ(Curveball::get_next_prime(1600), 1601ul);
}