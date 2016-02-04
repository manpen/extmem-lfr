#include <gtest/gtest.h>
#include <defs.h>
#include <Utils/AsyncStream.h>

class TestAsyncStreamStreamCentric : public ::testing::TestWithParam<uint_t> {};
class TestAsyncStreamBufferCentric : public ::testing::TestWithParam<uint_t> {};

TEST_P(TestAsyncStreamStreamCentric, orderedSequence) {
    uint_t sequenceLength = GetParam();

    struct CounterStream {
        uint_t _length;
        uint_t _current;

        CounterStream(uint_t length) : _length(length), _current(0) {}
        void operator++() {_current++;}
        const uint_t & operator*() const {return _current;}
        bool empty() const {return _current >= _length;}
    };

    CounterStream counterStream(sequenceLength);
    AsyncStream<CounterStream, uint_t> asyncStream(counterStream);

    for(unsigned int i=0; i < sequenceLength; i++, ++asyncStream) {
        ASSERT_FALSE(asyncStream.empty());
        ASSERT_EQ(i, *asyncStream);
    }

    ASSERT_TRUE(asyncStream.empty());
}

INSTANTIATE_TEST_CASE_P(TestAsyncStreamStreamCentricLengths,
    TestAsyncStreamStreamCentric,
    ::testing::Values(1, 2<<16, 2 << 20));

TEST_P(TestAsyncStreamBufferCentric, prematureConsumeStop) {
    uint_t buffers = GetParam();

    struct InfiniteStream {
        InfiniteStream& operator++() {return *this;}
        bool empty() const {return false;}
        uint_t operator*() const {return 1;}
    };

    InfiniteStream infiniteStream;
    AsyncStream<InfiniteStream, uint_t> asyncStream(infiniteStream, true, buffers);

    ++asyncStream;
}

INSTANTIATE_TEST_CASE_P(TestAsyncStreamBufferCentricSizes,
    TestAsyncStreamBufferCentric,
    ::testing::Values(3, 10, 100));
