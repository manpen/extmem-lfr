/**
 * Hung 30.08.16.
 */

#include <gtest/gtest.h>
#include <ConfigurationModel/ConfigurationModelRandom.h>
#include <Utils/StreamPusher.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <Utils/MonotonicPowerlawRandomStream.h>

class TestConfigurationModel : public ::testing::Test {
};

TEST_F(TestConfigurationModel, hhEdge) {
	const degree_t min_deg = 5;
	const degree_t max_deg = 20;
	const node_t num_nodes = 1000;
	const degree_t threshold = max_deg / 5;

	HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, threshold);
	MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

	StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
	hh_gen.generate();
}