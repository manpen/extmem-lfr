#include <gtest/gtest.h>
#include <defs.h>
#include <Utils/MonotonicUniformRandomStream.h>

class TestMonotonicUniformRandomStream : public ::testing::TestWithParam<std::tuple<uint_t, bool>> {};

TEST_P(TestMonotonicUniformRandomStream, basicProperties) {
    const uint_t length = std::get<0>(GetParam());
    const bool   increasing = std::get<1>(GetParam());
    MonotonicUniformRandomStream<true> rs(length);

    double last_rv = increasing ? 0.0 : 1.0;
    double sum = 0.0;

    for(uint_t i=0; i<length; i++, ++rs) {
        ASSERT_FALSE(rs.empty());

        if (increasing) {
            ASSERT_LE(last_rv, *rs); // monotony
        } else {
            ASSERT_GE(last_rv, *rs); // monotony
        }
        sum += *rs;
    }

    sum /= length;

    ASSERT_TRUE(rs.empty());
    EXPECT_LE(sum, 0.6);
    EXPECT_GE(sum, 0.4);
}

INSTANTIATE_TEST_CASE_P(TestMonotonicUniformRandomStreamSets,
                        TestMonotonicUniformRandomStream,
                        ::testing::Combine(
                            ::testing::Values(100, 1000000, 10000000),
                            ::testing::Bool()
                        )
);