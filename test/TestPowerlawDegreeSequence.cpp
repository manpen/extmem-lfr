/**
 * @file
 * @brief  Tests of the Powerlaw Degree Sequence Generator
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */
#include <gtest/gtest.h>

#include <Utils/MonotonicPowerlawRandomStream.h>
#include <defs.h>

class TestPowerlawDegreeSequence : public ::testing::Test {};

/**
 * @brief Checks that Powerlaw Degree Sequence is non-decreasing and OOB
 *
 * This non-decreasing property is used for Distribution Count (i.e. for reference stats
 * and the RLE Havel Hakimi) in order to save a sorting phase.
 */
TEST_F(TestPowerlawDegreeSequence, testNonDecreasing) {
    // repeat test 10 times -- each time with a different input size and different seed
    for(unsigned int i = 1; i <= 10; i++) {
        const int_t numNodes = IntScale::Mi * i;
        const degree_t minDeg = 2;
        const degree_t maxDeg = numNodes / 4;

        MonotonicPowerlawRandomStream<true> sequence(minDeg, maxDeg, -2.0, numNodes, 1.0, i);

        degree_t last_value = std::numeric_limits<degree_t>::min();

        // check every element of stream
        int_t numCounted = 0;
        for(; !sequence.empty(); ++sequence) {
            auto val = *sequence;

            EXPECT_GE(val, minDeg); // lower bound
            EXPECT_LE(val, maxDeg); // upper bound
            EXPECT_GE(val, last_value); // non-decreasing

            last_value = *sequence;

            ++numCounted;
        }

        // Verify that the number of elements produced matches the number of elements requested
        EXPECT_EQ(numCounted, numNodes);
    }
}

