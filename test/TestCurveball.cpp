/**
 * @file TestCurveball.cpp
 * @date 28. September 2017
 *
 * @author Hung Tran
 */

#include <gtest/gtest.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>

#include <Utils/StreamPusher.h>
#include "EdgeStream.h"
#include <Curveball/EMCurveball.h>
#include <DistributionCount.h>
#include <Utils/StreamPusherRedirectStream.h>
#include <Utils/Hashfuncs.h>
#include <Utils/NodeHash.h>

class TestCurveball : public ::testing::Test {
};

TEST_F(TestCurveball, pld_instance_without_paramest) {
	// Config
	const node_t num_nodes = 4000;
	const degree_t min_deg = 5;
	const degree_t max_deg = 100;
	const uint32_t num_rounds = 10;
	const Curveball::chunkid_t num_macrochunks = 8;
	const Curveball::chunkid_t num_batches = 8;
	const Curveball::chunkid_t num_fanout = 2;
	const Curveball::msgid_t num_max_msgs = std::numeric_limits<Curveball::msgid_t>::max();
	const int num_threads = 4;
	const size_t insertion_buffer_size = 128;

	// Build edge list
	EdgeStream edge_stream;
	EdgeStream out_edge_stream;

	HavelHakimiIMGeneratorWithDegrees hh_gen(
		HavelHakimiIMGeneratorWithDegrees::PushDirection::DecreasingDegree);
	MonotonicPowerlawRandomStream<false> degree_sequence(min_deg, max_deg, -2, num_nodes, 1.0, stxxl::get_next_seed());

	StreamPusher<decltype(degree_sequence), decltype(hh_gen)>(degree_sequence, hh_gen);
	hh_gen.generate();
	StreamPusher<decltype(hh_gen), EdgeStream>(hh_gen, edge_stream);
	hh_gen.finalize();

	DegreeStream &degree_stream = hh_gen.get_degree_stream();

	// Run algorithm
	edge_stream.rewind();
	degree_stream.rewind();
	Curveball::EMCurveball<Curveball::ModHash, EdgeStream> algo(edge_stream,
																degree_stream,
																num_nodes,
																num_rounds,
																out_edge_stream,
																num_macrochunks,
																num_batches,
																num_fanout,
																2 * Curveball::UIntScale::Gi,
																2 * Curveball::UIntScale::Gi,
																num_max_msgs,
																num_threads,
																insertion_buffer_size);

	algo.run();

	// Check edge count
	ASSERT_EQ(out_edge_stream.size(), edge_stream.size());

	// Check degrees
	stxxl::sorter<node_t, Curveball::NodeComparator> node_tokens(Curveball::NodeComparator{}, 2 * UIntScale::Gi);
	out_edge_stream.rewind();
	for (; !out_edge_stream.empty(); ++out_edge_stream) {
		const auto edge = *out_edge_stream;
		node_tokens.push(edge.first);
		node_tokens.push(edge.second);
	}
	node_tokens.sort();

	DistributionCount<decltype(node_tokens), size_t> token_count(node_tokens);
	degree_stream.rewind();
	for (; !token_count.empty(); ++token_count, ++degree_stream) {
		ASSERT_EQ(*degree_stream, static_cast<degree_t>((*token_count).count));
	}
}

TEST_F(TestCurveball, pld_instance_with_paramest) {
	// Config
	const node_t num_nodes = 4000;
	const degree_t min_deg = 5;
	const degree_t max_deg = 100;
	const uint32_t num_rounds = 10;
	const Curveball::chunkid_t num_macrochunks = 8;
	const Curveball::chunkid_t num_batches = 8;
	const Curveball::chunkid_t num_fanout = 2;
	const Curveball::msgid_t num_max_msgs = std::numeric_limits<Curveball::msgid_t>::max();
	const int num_threads = 4;
	const size_t insertion_buffer_size = 128;

	// Build edge list
	EdgeStream edge_stream;
	EdgeStream out_edge_stream;

	HavelHakimiIMGeneratorWithDegrees hh_gen(
		HavelHakimiIMGeneratorWithDegrees::PushDirection::DecreasingDegree);
	MonotonicPowerlawRandomStream<false> degree_sequence(min_deg, max_deg, -2, num_nodes, 1.0, stxxl::get_next_seed());

	StreamPusher<decltype(degree_sequence), decltype(hh_gen)>(degree_sequence, hh_gen);
	hh_gen.generate();
	StreamPusher<decltype(hh_gen), EdgeStream>(hh_gen, edge_stream);
	hh_gen.finalize();

	DegreeStream &degree_stream = hh_gen.get_degree_stream();

	// Run algorithm
	edge_stream.rewind();
	degree_stream.rewind();
	Curveball::EMCurveball<Curveball::ModHash, EdgeStream> algo(edge_stream,
																degree_stream,
																num_nodes,
																num_rounds,
																out_edge_stream,
																omp_get_max_threads(),
																8 * Curveball::UIntScale::Gi,
																true);

	algo.run();

	// Check edge count
	ASSERT_EQ(out_edge_stream.size(), edge_stream.size());

	// Check degrees
	stxxl::sorter<node_t, Curveball::NodeComparator> node_tokens(Curveball::NodeComparator{}, 2 * UIntScale::Gi);
	out_edge_stream.rewind();
	for (; !out_edge_stream.empty(); ++out_edge_stream) {
		const auto edge = *out_edge_stream;
		node_tokens.push(edge.first);
		node_tokens.push(edge.second);
	}
	node_tokens.sort();

	DistributionCount<decltype(node_tokens), size_t> token_count(node_tokens);
	degree_stream.rewind();
	for (; !token_count.empty(); ++token_count, ++degree_stream) {
		ASSERT_EQ(*degree_stream, static_cast<degree_t>((*token_count).count));
	}
}
