//
// Created by michael on 28.07.15.
//
#include <gtest/gtest.h>

#include <stxxl/random>
#include <stxxl/priority_queue>

namespace {
class TestPriorityQueue : public ::testing::Test {};

struct myElement {
    stxxl::int64 sortKey;
    stxxl::int64 id;
};


inline std::ostream &operator<<(std::ostream &os, myElement const &m) {
    return os << "{sortKey:" << m.sortKey << ", id:" << m.id << "}";
}

TEST_F(TestPriorityQueue, testEqualRange) {
    stxxl::int64 numElements = 100 * 1000;
    stxxl::int64 maxKey = 256;

    stxxl::random_number32 rand;

    struct MyCompareLess {
        bool operator()(const myElement &a, const myElement &b) const { return a.sortKey < b.sortKey; };
        myElement min_value() const { return {std::numeric_limits<stxxl::int64>::min(), std::numeric_limits<stxxl::int64>::min()}; };
    };


    typedef stxxl::PRIORITY_QUEUE_GENERATOR<myElement, MyCompareLess, 16*1024*1024, 1024*1024>::result pqueue_type;
    typedef pqueue_type::block_type block_type;
    stxxl::read_write_pool<block_type> pool(static_cast<stxxl::read_write_pool<block_type>::size_type>(8*1024*1024/block_type::raw_size),
                                            static_cast<stxxl::read_write_pool<block_type>::size_type>(8*1024*1024/block_type::raw_size));
    pqueue_type prioQueue(pool);

    for (stxxl::int64 i = 0; i < numElements; ++i) {
        prioQueue.push({rand(maxKey), i});
    }

    std::vector<bool> idCheck(numElements, false);

    while (!prioQueue.empty()) {
        EXPECT_FALSE(idCheck[prioQueue.top().id]);
        idCheck[prioQueue.top().id] = true;
        prioQueue.pop();
    }
}
}
