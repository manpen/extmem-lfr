/**
 * Hung 30.08.16.
 */

#include <gtest/gtest.h>

#include <iomanip>

#include <ConfigurationModel.h>

class TestConfigurationModel : public ::testing::Test {
};

TEST_F(TestConfigurationModel, multiNodeMsgComparator) {
	// we try 2^15 + 1 as seed (should allow more TODO)
	const uint32_t seed = (1 << 15) | 1;

	MultiNodeMsgComparator comp{seed};

	uint64_t max = comp.max_value();

	uint32_t potmax_msb = static_cast<uint32_t>(max >> 32);
	uint32_t potmax_lsb = static_cast<uint32_t>(max);

	// observation here: if not inverted potmax_msb below then potmax_lsb == seed (crc bilinear?)
	std::cout << "MSBs: " << std::hex << potmax_msb << ", LSBs: " << std::hex << potmax_lsb << std::endl;

	// Test if max_value really found
	ASSERT_EQ(_mm_crc32_u32(seed, potmax_msb), UINT32_MAX);
	ASSERT_EQ(_mm_crc32_u32(~potmax_msb, potmax_lsb), UINT32_MAX);

	// Test if some random crc_hashes are smaller;
	MultiNodeMsg msg1{1};
	MultiNodeMsg msg2{2};
	MultiNodeMsg msg3{100};

	MultiNodeMsg msgmax{max};

	ASSERT_TRUE(comp(msg1, msgmax));
	ASSERT_TRUE(comp(msg2, msgmax));
	ASSERT_TRUE(comp(msg3, msgmax));
}
