/**
 * Hung 30.08.16.
 */

#include <gtest/gtest.h>

#include <ConfigurationModel.h>

class TestConfigurationModel : public ::testing::Test {
};

TEST_F(TestConfigurationModel, multiNodeMsgComparator) {
	// we try 1 as seed (should allow more TODO)
	const uint32_t seed = 1;

	MultiNodeMsgComparator comp{seed};

	uint32_t potmax_msb = static_cast<uint32_t>(comp.max_value() >> 32);
	uint32_t potmax_lsb = static_cast<uint32_t>(comp.max_value());
	
	ASSERT_EQ(_mm_crc32_u32(seed, potmax_msb), std::numeric_limits<uint32_t>::max());
	ASSERT_EQ(_mm_crc32_u32(potmax_msb, potmax_lsb), std::numeric_limits<uint32_t>::max());
}
