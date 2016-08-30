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
	uint64_t min = comp.min_value();

	uint32_t potmax_msb = static_cast<uint32_t>(max >> 32);
	uint32_t potmax_lsb = static_cast<uint32_t>(max);
	uint32_t potmin_msb = static_cast<uint32_t>(min >> 32);
	uint32_t potmin_lsb = static_cast<uint32_t>(min);


	// observation here: if not inverted potmax_msb below then potmax_lsb == seed (crc bilinear?)
	std::cout << ".....max MSBs: " << std::hex << potmax_msb << ", LSBs: " << std::hex << potmax_lsb << std::endl;
	std::cout << "hash max MSBs: " << std::hex << _mm_crc32_u32(seed, potmax_msb) << ", LSBs: " << std::hex << _mm_crc32_u32(~potmax_msb, potmax_lsb) << std::endl;
	std::cout << ".....min MSBs: " << std::hex << potmin_msb << ", LSBs: " << std::hex << potmin_lsb << std::endl;
	std::cout << "hash min MSBs: " << std::hex << _mm_crc32_u32(seed, potmin_msb) << ", LSBs: " << std::hex << _mm_crc32_u32(~potmin_msb, potmin_lsb) << std::endl;

	// Test if max_value and min_value really found
	ASSERT_EQ(_mm_crc32_u32(seed, potmax_msb), UINT32_MAX);
	ASSERT_EQ(_mm_crc32_u32(~potmax_msb, potmax_lsb), UINT32_MAX);
	ASSERT_EQ(_mm_crc32_u32(seed, potmin_msb), static_cast<uint32_t>(0));
	ASSERT_EQ(_mm_crc32_u32(~potmin_msb, potmin_lsb), static_cast<uint32_t>(0));

	// Test if some crc_hashes are smaller than max_value() and greater than min_value();
	MultiNodeMsg msg1{1};
	MultiNodeMsg msg2{2};
	MultiNodeMsg msg3{100};

	MultiNodeMsg msgmax{max};
	MultiNodeMsg msgmin{min};

	ASSERT_TRUE(comp(msg1, msgmax));
	ASSERT_TRUE(comp(msg2, msgmax));
	ASSERT_TRUE(comp(msg3, msgmax));

	ASSERT_FALSE(comp(msg1, msgmin));
	ASSERT_FALSE(comp(msg2, msgmin));
	ASSERT_FALSE(comp(msg3, msgmin));
}

TEST_F(TestConfigurationModel, algoClass) {
	auto degrees = MonotonicPowerlawRandomStream<false>(1, (1<<9), 2, (1<<14));

	std::vector<degree_t> ref_degrees;
	stxxl::stream::materialize(degrees, ref_degrees.begin());

	auto degrees_cm = stxxl::stream::streamify(ref_degrees.begin(), ref_degrees.end());

	//TODO
}

