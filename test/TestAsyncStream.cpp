#include <gtest/gtest.h>
#include <defs.h>
#include <Utils/AsyncStream.h>

class TestAsyncStreamStreamCentric : public ::testing::TestWithParam<std::tuple<bool,uint_t, uint_t>> {};
class TestAsyncStreamBufferCentric : public ::testing::TestWithParam<uint_t> {};

TEST_P(TestAsyncStreamStreamCentric, orderedSequence) {
    bool   blockAccess    = std::get<0>(GetParam());
    uint_t sequenceLength = std::get<1>(GetParam());
    uint_t numberOfBuffers= std::get<2>(GetParam());

    struct CounterStream {
        const uint_t _length;
        uint_t _current;

        CounterStream(uint_t length) : _length(length), _current(0) {}
        void operator++() {_current++;}
        const uint_t & operator*() const {return _current;}
        bool empty() const {return _current >= _length;}
    };

    CounterStream counterStream(sequenceLength);
    unsigned int block_size = 30000;
    AsyncStream<CounterStream, uint_t> asyncStream(counterStream, true, block_size, numberOfBuffers);

    if (blockAccess) {
        unsigned int i = 0;

        while(i < sequenceLength) {
            ASSERT_FALSE(asyncStream.empty());
            auto &buffer = asyncStream.readBuffer();
            for (auto &b : buffer) {
                ASSERT_EQ(i++, b);
            }
            asyncStream.nextBuffer();
        }
    } else {
        for (unsigned int i = 0; i < sequenceLength; i++, ++asyncStream) {
            ASSERT_FALSE(asyncStream.empty());
            ASSERT_EQ(i, *asyncStream);
        }
    }

    ASSERT_TRUE(asyncStream.empty());
}

INSTANTIATE_TEST_CASE_P(TestAsyncStreamStreamCentricLengths,
    TestAsyncStreamStreamCentric,
    ::testing::Combine(::testing::Bool(), ::testing::Values(0, 1, 2<<16, 2 << 20), ::testing::Values(2,3,4)));

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
