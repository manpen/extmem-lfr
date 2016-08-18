#include <gtest/gtest.h>
#include <defs.h>
#include <Utils/MonotonicPowerlawRandomStream.h>

class TestMonotonicPowerlawRandomStream :
    public ::testing::TestWithParam<std::tuple<uint_t, uint_t, bool>> {};

TEST_P(TestMonotonicPowerlawRandomStream, basicProperties) {
    const degree_t min        = std::get<0>(GetParam());
    const degree_t length     = std::get<1>(GetParam());
    const bool   increasing = std::get<2>(GetParam());

    const degree_t max = 10000;

    MonotonicPowerlawRandomStream<true> rs(min, max, -2.0, length);

    degree_t last_rv = increasing ? min : max;
    for(degree_t i=0; i<length; i++, ++rs) {
        ASSERT_FALSE(rs.empty());

        if (increasing) {
            ASSERT_LE(last_rv, *rs); // monotony
        } else {
            ASSERT_GE(last_rv, *rs); // monotony
        }

        ASSERT_LE(*rs, max);
        ASSERT_GE(*rs, min);
    }

    ASSERT_TRUE(rs.empty());
}

INSTANTIATE_TEST_CASE_P(TestMonotonicPowerlawRandomStreamSets,
                        TestMonotonicPowerlawRandomStream,
                        ::testing::Combine(
                            ::testing::Values(1, 10),
                            ::testing::Values(10, 100000, 10000000),
                            ::testing::Bool()
                        )
);