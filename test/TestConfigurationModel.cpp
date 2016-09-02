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

	ASSERT_TRUE(comp(msgmin, msgmax));

	ASSERT_TRUE(comp(msg1, msgmax));
	ASSERT_TRUE(comp(msg2, msgmax));
	ASSERT_TRUE(comp(msg3, msgmax));

	ASSERT_FALSE(comp(msg1, msgmin));
	ASSERT_FALSE(comp(msg2, msgmin));
	ASSERT_FALSE(comp(msg3, msgmin));
}

TEST_F(TestConfigurationModel, algoClass) {
	// test sorter
	auto degrees = MonotonicPowerlawRandomStream<false>(1, (1<<9), -2, (1<<14));
	
	ASSERT_FALSE(degrees.empty());

	ConfigurationModel<> cm(degrees, static_cast<uint32_t>((2 << 15) + 1));
	cm.run();

	ASSERT_FALSE(cm.empty());

	// test scan through, check if ordered right
	int count = 1;

	auto prev_edge = *cm;
	
	++cm;

	bool correct = prev_edge.first <= prev_edge.second;

	for(; !cm.empty(); ++count, ++cm) {
		const auto & edge = *cm;

		correct = correct & (edge.first <= edge.second) & (edge.first >= prev_edge.first);

		prev_edge = edge;
	}

	ASSERT_TRUE(correct);

	std::cout << "COUNT: " << count << std::endl;

	ASSERT_GT(count, (1<<14));

	ASSERT_TRUE(cm.empty());

	cm.clear();

	// test pusher
	auto degrees2 = MonotonicPowerlawRandomStream<false>(1, (1<<9), -2, (1<<14));

	ASSERT_FALSE(degrees2.empty());

	std::vector<degree_t> ref_degrees (1<<14);

	stxxl::stream::materialize(degrees2, ref_degrees.begin());

	ASSERT_EQ(ref_degrees.size(), static_cast<uint32_t>(1<<14));

	ASSERT_TRUE(degrees2.empty());

	auto degree_stream = stxxl::stream::streamify(ref_degrees.begin(), ref_degrees.end());

	ASSERT_FALSE(degree_stream.empty());
}

TEST_F(TestConfigurationModel, outputAnalysis) {
	int loopCount = 0;
	int multiEdges_singleCount = 0;
	int multiEdges_multiCount = 0;
	
	// we do 10 runs here.., with i as seed
	for (uint32_t i = 1; i <= 10; ++i) {
		auto degrees = MonotonicPowerlawRandomStream<false>(1, (1<<9), -2, (1<<14));

		ASSERT_FALSE(degrees.empty());

		ConfigurationModel<> cm(degrees, i);
		cm.run();

		ASSERT_FALSE(cm.empty());

		bool prev_multi = false;

		auto prev_edge = *cm;
		++cm;

		if (prev_edge.is_loop()) 
			++loopCount;

		for (; !cm.empty(); ++cm) {
			const auto & edge = *cm;
			//std::cout << "Edge: <" << edge.first << ", " << edge.second << ">" << std::endl;	

			if (edge.is_loop()) {
				++loopCount;
				prev_edge = edge;
				prev_multi = false;
				continue;
			}
			if (prev_edge == edge) {
				++multiEdges_multiCount;

				if (!prev_multi) {
					++multiEdges_singleCount;
					prev_multi = true;
				}

				prev_edge = edge;

				continue;
			}

			prev_edge = edge;

			prev_multi = false;
		}

		// outputmessage
		std::cout << "RUN[" << i << "]:" << std::endl;
		std::cout << "         # SELF_LOOPS: " << loopCount << std::endl;
		std::cout << "        # MULTI_EDGES: " << multiEdges_singleCount << std::endl;
		std::cout << "# EDGES_IN_MULTI_EDGE: " << multiEdges_multiCount << std::endl;
		std::cout << "=======================" << std::endl;

		ASSERT_TRUE(cm.empty());

		cm.clear();

		loopCount = 0;
		multiEdges_multiCount = 0;
		multiEdges_singleCount = 0;
	}
}
