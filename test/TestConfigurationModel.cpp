/**
 * Hung 30.08.16.
 */

#include <gtest/gtest.h>

#include <iomanip>

#include <ConfigurationModel.h>

#include <string>

#include <iostream>

class TestConfigurationModel : public ::testing::Test {
};

TEST_F(TestConfigurationModel, findInverse) {
	bool found = false;
	for (uint32_t i = 1; i < UINT32_MAX; ++i) {
		if (_mm_crc32_u32(static_cast<uint32_t>(0), i) == 0xFFFFFFFF){
			std::cout << "POLY: " << std::hex << i << std::endl;
			found = true;
		}
	}

	ASSERT_TRUE(found);

	// WE FOUND THE CONSTANT:
	// from line 5 in intel intrinsics of _mm_crc32_u32:
	// tmp5[95:0] = 0x4546F146|00000000
	// therefore the seed XOR 0x641F6454, endianness is important here...
	// is the inverse! WE TEST:
	for (uint32_t seed = 0; seed < UINT32_MAX; ++seed) {
		uint32_t inverse_to_seed = reinterpret_cast<uint32_t>(0x641F6454 ^ seed);
		ASSERT_EQ(reinterpret_cast<uint32_t>(inverse_to_seed ^ seed), static_cast<uint32_t>(0x641F6454));
		ASSERT_EQ(_mm_crc32_u32(seed, inverse_to_seed), static_cast<uint32_t>(0xFFFFFFFF));	
	}
	// the constant for min_value is simply 0x00000000.
}

TEST_F(TestConfigurationModel, searchMax) {
	for (uint32_t i = 0; i < UINT32_MAX; ++i) {
		if (_mm_crc32_u32(0xFFFFFFFF, i) == 0xFFFFFFFF) {
			std::cout << std::hex << "MAXI" << i << std::endl;
		}
	}
}

TEST_F(TestConfigurationModel, comparator) {
	uint32_t test_Max = _mm_crc32_u32(0xFFFFFFFF, LIMITS_LSB);

	ASSERT_EQ(test_Max, 0xFFFFFFFF);
	
	MultiNodeMsgComparator mnmc(static_cast<uint32_t>(1<<3));
	
	auto degrees = MonotonicPowerlawRandomStream<false>(1, (1<<9), -2, (1<<19));

	ConfigurationModel<> cm(degrees, static_cast<uint32_t>((1<<3)));
	cm.run();	
	
	ASSERT_FALSE(cm.empty());

	ASSERT_TRUE(true);

	auto degrees2 = MonotonicPowerlawRandomStream<false>(10, (1<<5), -2, (1<<15));

	std::vector<degree_t> ref_degrees (1<<15);

	stxxl::stream::materialize(degrees2, ref_degrees.begin());

	auto deg_stream = stxxl::stream::streamify(ref_degrees.begin(), ref_degrees.end());

	//ConfigurationModel<stxxl::stream::vector_iterator2stream<stxxl::vector<degree_t>::const_iterator>> cm2(deg_stream, static_cast<uint32_t>(1<<3));
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

TEST_F(TestConfigurationModel, finalOutput) {
	// we do 100 Runs
	constexpr uint32_t dmin = 1;
	constexpr uint32_t dmax = (1<<9);
	constexpr uint32_t gamma = 2;
	constexpr uint32_t n = (1<<14);
	constexpr uint32_t c = 1;

	std::ostringstream stringStream;
	stringStream << "data_";
	stringStream << std::to_string(c);
	stringStream << "_";
	stringStream << std::to_string(dmin);
	stringStream << "_";
	stringStream << std::to_string(dmax);
	stringStream << "_";
	stringStream << std::to_string(gamma);
	stringStream << "_";
	stringStream << std::to_string(n);
	stringStream << ".txt";
	std::string copyOfStr = stringStream.str();

	std::ofstream outputFile(copyOfStr);
	
	std::cout << "GOING" << std::endl;
	
	for (uint32_t i = 0; i < c; ++i) {
		uint64_t loopCount = 0;
		uint64_t me_single = 0;
		uint64_t me_multi  = 0;

		auto degrees = MonotonicPowerlawRandomStream<false>(dmin, dmax, -gamma, n);

		ASSERT_FALSE(degrees.empty());

		ConfigurationModel<> cm(degrees, i);
		cm.run();

		ASSERT_FALSE(cm.empty());

		auto prev_edge = *cm;
		++cm;

		bool prev_multi = false;


		if (prev_edge.is_loop())
			++loopCount;

		for(; !cm.empty(); ++cm) {
			auto & edge = *cm;

			if (edge.is_loop()) {
				++loopCount;
				prev_edge = edge;
				prev_multi = false;
				continue;
			}
			if (prev_edge == edge) {
				++me_multi;

				if (!prev_multi) {
					++me_single;
					prev_multi = true;
				}

				prev_edge = edge;

				continue;
			}

			prev_edge = edge;

			prev_multi = false;

		}
	
		cm.clear();

		std::ostringstream stringStream2;
		stringStream2 << std::to_string(loopCount);
		stringStream2 << "\t";
		stringStream2 << std::to_string(me_single);
		stringStream2 << "\t";
		stringStream2 << std::to_string(me_multi);
		stringStream2 << " #LSM\n";

		std::string lineInfo = stringStream2.str();

		std::cout << lineInfo << std::endl;

		outputFile << lineInfo;

		stringStream2.str(std::string());
	}
}

TEST_F(TestConfigurationModel, outputAnalysis) {
	int loopCount = 0;
	int multiEdges_singleCount = 0;
	int multiEdges_multiCount = 0;
	
	// we do 10 runs here.., with i as seed
	for (uint32_t i = 1; i <= 1; ++i) {
		auto degrees = MonotonicPowerlawRandomStream<false>(1, (1<<9), -2, (1<<14));

		ASSERT_FALSE(degrees.empty());

		ConfigurationModel<> cm(degrees, (1<<16)+i);
		cm.run();

		ASSERT_FALSE(cm.empty());

		bool prev_multi = false;

		auto prev_edge = *cm;
		++cm;
		std::cout << "EDGE<" << prev_edge.first << ", " << prev_edge.second << ">" << std::endl;


		if (prev_edge.is_loop()) 
			++loopCount;

		for (; !cm.empty(); ++cm) {
			const auto & edge = *cm;
			//std::cout << "Edge: <" << edge.first << ", " << edge.second << ">" << std::endl;	
		
			std::cout << "EDGE<" << edge.first << ", " << edge.second << ">" << std::endl;


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

