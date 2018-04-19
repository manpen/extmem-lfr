/**
 * @file main_curveball_benchmark.cpp
 * @date 22. October 2017
 *
 * @author Hung Tran
 */

#include <iostream>
#include <chrono>
#include <EdgeStream.h>
#include <stxxl/cmdline>
#include <Curveball/EMCurveball.h>

#include <HavelHakimi/HavelHakimiIMGenerator.h>

#include <Utils/StreamPusher.h>
#include <Utils/IOStatistics.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <Utils/NodeHash.h>
#include <DegreeStream.h>
#include <Utils/StreamPusherRedirectStream.h>

struct PowerlawBenchmarkParams {
	uint32_t num_rounds;
	stxxl::uint64 num_nodes;
	stxxl::uint64 min_deg;
	stxxl::uint64 max_deg;
	double gamma;
	stxxl::uint64 internal_mem;
	int num_threads;
	unsigned int random_seed;
	uint32_t num_macrochunks;
	uint32_t num_microchunk_splits;
	uint32_t num_batch_splits;
	stxxl::uint64 insertion_buffer_size;
	stxxl::uint64 num_max_msgs;

	PowerlawBenchmarkParams() :
	  num_rounds(1),
	  num_nodes(10 * UIntScale::M),
	  min_deg(2),
	  max_deg(100 * UIntScale::K),
	  gamma(-2.0),
	  internal_mem(2 * UIntScale::Gi),
	  num_threads(16),
	  num_macrochunks(16),
	  num_microchunk_splits(16),
		num_batch_splits(1),
	  insertion_buffer_size(1000),
	  num_max_msgs(Curveball::DUMMY_LIMIT) // not a concern
	{
	  using my_clock = std::chrono::high_resolution_clock;
	  my_clock::duration d = my_clock::now() - my_clock::time_point::min();
	  random_seed = d.count();
	}

#if STXXL_VERSION_INTEGER > 10401
#define CMDLINE_COMP(chr, str, dest, args...) \
		chr, str, dest, args
#else
		#define CMDLINE_COMP(chr, str, dest, args...) \
		chr, str, args, dest
#endif

		bool parse_cmdline(int argc, char* argv[]) {
			stxxl::cmdline_parser cp;
			{
				cp.add_uint(CMDLINE_COMP('r', "num_rounds", num_rounds, "Number of Global Trade Rounds"));
				cp.add_bytes(CMDLINE_COMP('n', "num_nodes", num_nodes, "Number of Nodes"));
				cp.add_bytes(CMDLINE_COMP('a', "min_deg", min_deg, "Min. Degree of Powerlaw Degree Distribution"));
				cp.add_bytes(CMDLINE_COMP('b', "max_deg", max_deg, "Max. Degree of Powerlaw Degree Distribution"));
				cp.add_double(CMDLINE_COMP('g', "gamma", gamma, "Gamma of Powerlaw Degree Distribution"));
				cp.add_bytes(CMDLINE_COMP('i', "ram", internal_mem, "Internal Memory"));
				cp.add_int(CMDLINE_COMP('t', "num_threads", num_threads, "Number of Threads"));
				cp.add_uint(CMDLINE_COMP('s', "seed", random_seed, "Initial Seed for PRNG"));
				cp.add_uint(CMDLINE_COMP('c', "num_macrochunks", num_macrochunks, "Number of Macrochunks"));
				cp.add_uint(CMDLINE_COMP('z', "num_microchunk_splits", num_microchunk_splits, "Number of Microchunks Multiplier in a Macrochunk"));
				cp.add_uint(CMDLINE_COMP('j', "num_batch_splits", num_batch_splits, "Number of Microchunk Multiplier in a Batch"));
				cp.add_bytes(CMDLINE_COMP('y', "insertion_buffer_size", insertion_buffer_size, "Insertion Buffer Size"));
				cp.add_bytes(CMDLINE_COMP('l', "num_max_msgs", num_max_msgs, "Number of Max. Messages in RAM"));

				if (!cp.process(argc, argv)) {
					cp.print_usage();
					return false;
				}
			}

			cp.print_result();
			return true;
		}
};

void benchmark(const PowerlawBenchmarkParams& config) {
	stxxl::stats *stats = stxxl::stats::get_instance();
	stxxl::stats_data stats_begin(*stats);

	// Build edge list
	EdgeStream edge_stream;
    EdgeStream out_edge_stream;
    edgeid_t edge_count;

        IOStatistics hh_report;

        HavelHakimiIMGeneratorWithDegrees hh_gen(
        HavelHakimiIMGeneratorWithDegrees::PushDirection::DecreasingDegree);
        MonotonicPowerlawRandomStream<false> degree_sequence(config.min_deg,
                                                             config.max_deg,
                                                             config.gamma,
                                                             config.num_nodes,
                                                             1.0,
                                                             stxxl::get_next_seed());

        StreamPusher<decltype(degree_sequence), decltype(hh_gen)>(degree_sequence, hh_gen);
        hh_gen.generate();
        StreamPusher<decltype(hh_gen), EdgeStream>(hh_gen, edge_stream);

        edge_count =
            hh_gen.maxEdges() - hh_gen.unsatisfiedDegree();

        hh_gen.finalize();
        DegreeStream& degree_stream = hh_gen.get_degree_stream();

        hh_report.report("HHEdges");

        // Run algorithm
        edge_stream.rewind();
        degree_stream.rewind();
        IOStatistics cb_report;
        Curveball::EMCurveball<Curveball::ModHash, EdgeStream> algo(edge_stream,
                                                    degree_stream,
                                                    config.num_nodes,
                                                    config.num_rounds,
                                                    out_edge_stream,
                                                    config.num_macrochunks,
                                                    config.num_microchunk_splits,
                                                    config.num_batch_splits,
                                                    config.internal_mem / 4,
                                                    config.internal_mem,
                                                    config.num_max_msgs,
                                                    config.num_threads,
                                                    config.insertion_buffer_size);

        algo.run();
        cb_report.report("CurveballStats");

        std::cout << "Initial edgecount " << edge_count << std::endl;
        std::cout << "Output edgecount " << out_edge_stream.size() << std::endl;

        if (edge_count != out_edge_stream.size())
            std::cout << "Error" << std::endl;
}

int main(int argc, char* argv[]) {
	#ifndef NDEBUG
	std::cout << "[Built with assertions]" << std::endl;
	#endif
	std::cout << "STXXL VERSION" << STXXL_VERSION_INTEGER << std::endl;

	// print arguments
	for (int i = 0; i < argc; ++i)
		std::cout << argv[i] << " ";
	std::cout << std::endl;

	// infos regarding data types
	std::cout << "int_t:         " << sizeof(int_t) << "b\n"
					"uint_t:        " << sizeof(uint_t) << "b\n"
					          "node_t:        " << sizeof(node_t) << "b\n"
					          "degree_t:      " << sizeof(degree_t) << "b\n"
					          "edge_t:        " << sizeof(edge_t) << "b\n"
					          "NeighbourMsg:  " << sizeof(Curveball::NeighbourMsg) << "b\n"
	          << std::endl;

	PowerlawBenchmarkParams config;
	if (!config.parse_cmdline(argc, argv))
		return -1;

	stxxl::srandom_number32(config.random_seed);
	stxxl::set_seed(config.random_seed);

	benchmark(config);
	std::cout << "Maximum EM allocation: " << stxxl::block_manager::get_instance()->get_maximum_allocation() << std::endl;

	return 0;
}
