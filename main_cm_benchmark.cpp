//
// Created by hungt on 28.12.16.
//

#include <iostream>
#include <chrono>

#include <algorithm>
#include <locale>

#include <stxxl/cmdline>

#include <stack>
#include <stxxl/vector>
#include <EdgeStream.h>

#include <Utils/IOStatistics.h>

#include <Utils/MonotonicPowerlawRandomStream.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <Utils/StreamPusher.h>


#include <DegreeDistributionCheck.h>
#include "SwapGenerator.h"

#include <EdgeSwaps/EdgeSwapParallelTFP.h>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/EdgeSwapTFP.h>
#include <EdgeSwaps/IMEdgeSwap.h>

#include <ConfigurationModel.h>
#include <SwapStream.h>
#include <EdgeSwaps/ModifiedEdgeSwapTFP.h>
#include <Utils/export_metis.h>

struct RunConfig {
    unsigned int end;
    stxxl::uint64 minDeg;
    double ratio;
    double threshold_div;
    double gamma;

    unsigned int randomSeed;

    unsigned int start;

    RunConfig()
            : end(5)
            , minDeg(10)
            , ratio(10.0)
            , threshold_div(10.0)
            , gamma(-2.0)
            , start(2) {
        using myclock = std::chrono::high_resolution_clock;
        myclock::duration d = myclock::now() - myclock::time_point::min();
        randomSeed = d.count();
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
        std::string swap_algo_name;

        // setup and gather parameters
        {
            cp.add_uint  (CMDLINE_COMP('n', "end-power", end,         "Generate to 10^# nodes, Default: 10^7"));
            cp.add_bytes (CMDLINE_COMP('a', "min-deg",   minDeg,      "Min. Deg of Powerlaw Deg. Distr."));
            cp.add_double(CMDLINE_COMP('g', "gamma",     gamma,       "Gamma of Powerlaw Deg. Distr."));
            cp.add_double(CMDLINE_COMP('r', "ratio",     ratio,       "Divisor for MaxDegree"));
            cp.add_double(CMDLINE_COMP('x', "threshold_div", threshold_div, "Thresholddivider"));
            cp.add_uint  (CMDLINE_COMP('s', "seed",      randomSeed,  "Initial seed for PRNG"));
            cp.add_uint  (CMDLINE_COMP('t', "start",     start,       "Start from 10^# nodes, Default: 10^2"));

            if (!cp.process(argc, argv)) {
                cp.print_usage();
                return false;
            }
        }

        cp.print_result();
        return true;
    }
};

void benchmark(RunConfig & config) {

    // CRC

    std::cout << config.threshold_div << std::endl;

    for (node_t n = pow(10, config.start); n <= pow(10, config.end); n *= 10) {
        const degree_t maxDeg = static_cast<degree_t >(n / config.ratio);
        const degree_t threshold = static_cast<degree_t>(maxDeg / config.threshold_div);

        HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, threshold);
        MonotonicPowerlawRandomStream<false> degreeSequence(config.minDeg, maxDeg, config.gamma, n);

        StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
        hh_gen.generate();

        std::cout << "Max degree fed to HH: " << hh_gen.maxDegree() << std::endl;
        std::cout << "Threshold: " << threshold << std::endl;
        std::cout << "Nodes with degree above threshold: " << n - hh_gen.nodesAboveThreshold() << std::endl;

        HavelHakimi_ConfigurationModel<HavelHakimiIMGenerator>
                cmhh(hh_gen, config.randomSeed, n, threshold, hh_gen.maxDegree(), n - hh_gen.nodesAboveThreshold());

        stxxl::stats *stats = stxxl::stats::get_instance();
        stxxl::stats_data stats_begin(*stats);

        cmhh.run();

        std::stringstream fmt;
        fmt << "cm_crc" << maxDeg << ".log";
        std::string fileName = fmt.str();
        std::ofstream cmhh_file(fileName);

        // End benchmark
        cmhh_file << "min_deg set to: " << config.minDeg << std::endl;
        cmhh_file << "max_deg set to: " << maxDeg << std::endl;
        cmhh_file << "edges set to: " << cmhh.size() << std::endl;
        cmhh_file << "nodes set to: " << n << std::endl;
        cmhh_file << (stxxl::stats_data(*stats) - stats_begin);
        //outLine.str(std::string());

        // count self-loops and multi-edges
        long loops = 0;
        long multi = 0;
        long times = 0;

        bool prev_multi = false;

        auto prev_edge = *cmhh;
        if (prev_edge.is_loop())
            ++loops;

        ++cmhh;

        for (; !cmhh.empty(); ++cmhh) {
            auto & edge = *cmhh;

            // self-loop found
            if (edge.is_loop()) {
                ++loops;
                if (prev_multi)
                    ++times;
                prev_edge = edge;
                prev_multi = false;
                continue;
            }

            // multi-edge found
            if (prev_edge == edge) {
                ++times;
                if (!prev_multi) {
                    ++multi;
                    prev_multi = true;
                }
                prev_edge = edge;
                continue;
            }

            if (prev_multi)
                ++times;
            prev_edge = edge;

            prev_multi = false;
        }

        if (prev_multi)
            ++times;

        cmhh_file << "self_loops: " << loops << std::endl;
        cmhh_file << "multi_edges: " << multi << std::endl;
        cmhh_file << "multi_edges quantities: " << times << std::endl;

        cmhh.clear();

        cmhh_file.close();
    }

    // Random
/*
    for (node_t n = pow(10, config.start); n <= pow(10, config.end); n *= 10) {
        const degree_t maxDeg = static_cast<degree_t >(n / config.ratio);
        const degree_t threshold = static_cast<degree_t>(maxDeg / config.ratio); // Here threshold_divisor previously

        HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, threshold);
        MonotonicPowerlawRandomStream<false> degreeSequence(config.minDeg, maxDeg, config.gamma, n);

        StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
        hh_gen.generate();

        std::cout << "Max degree fed to HH: " << hh_gen.maxDegree() << std::endl;
        std::cout << "Threshold: " << threshold << std::endl;
        std::cout << "Nodes with degree above threshold: " << hh_gen.nodesAboveThreshold() << std::endl;

        HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator, TestNodeRandomComparator>
                cmhh(hh_gen);

        stxxl::stats *stats = stxxl::stats::get_instance();
        stxxl::stats_data stats_begin(*stats);

        cmhh.run();

        std::stringstream fmt;
        fmt << "cm_tupr" << maxDeg << ".log";
        std::string fileName = fmt.str();
        std::ofstream cmhh_file(fileName);

        // End benchmark
        cmhh_file << "min_deg set to: " << config.minDeg << std::endl;
        cmhh_file << "max_deg set to: " << maxDeg << std::endl;
        cmhh_file << "edges set to: " << cmhh.size() << std::endl;
        cmhh_file << "nodes set to: " << n << std::endl;
        cmhh_file << (stxxl::stats_data(*stats) - stats_begin);
        //outLine.str(std::string());

        // count self-loops and multi-edges
        long loops = 0;
        long multi = 0;
        long times = 0;

        bool prev_multi = false;

        auto prev_edge = *cmhh;
        if (prev_edge.is_loop())
            ++loops;

        ++cmhh;

        for (; !cmhh.empty(); ++cmhh) {
            auto & edge = *cmhh;

            // self-loop found
            if (edge.is_loop()) {
                ++loops;
                if (prev_multi)
                    ++times;
                prev_edge = edge;
                prev_multi = false;
                continue;
            }

            // multi-edge found
            if (prev_edge == edge) {
                ++times;
                if (!prev_multi) {
                    ++multi;
                    prev_multi = true;
                }
                prev_edge = edge;
                continue;
            }

            if (prev_multi)
                ++times;
            prev_edge = edge;

            prev_multi = false;
        }

        if (prev_multi)
            ++times;

        cmhh_file << "self_loops: " << loops << std::endl;
        cmhh_file << "multi_edges: " << multi << std::endl;
        cmhh_file << "multi_edges quantities: " << times << std::endl;

        cmhh.clear();

        cmhh_file.close();
    }*/
}

int main(int argc, char* argv[]) {
#ifndef NDEBUG
    std::cout << "[build with assertions]" << std::endl;
#endif
    std::cout << "STXXL VERSION: " << STXXL_VERSION_INTEGER << std::endl;

    // nice to have in logs to restart it easier
    for(int i=0; i < argc; ++i)
        std::cout << argv[i] << " ";
    std::cout << std::endl;

    // infos regarding data types
    std::cout << "int_t:         " << sizeof(int_t) << "b\n"
            "uint_t:        " << sizeof(uint_t) << "b\n"
                      "node_t:        " << sizeof(node_t) << "b\n"
                      "degree_t:      " << sizeof(degree_t) << "b\n"
                      "community_t:   " << sizeof(community_t) << "b\n"
                      "edge_t:        " << sizeof(edge_t) << "b\n"
                      "swapid_t:      " << sizeof(swapid_t) << "b\n"
                      "SwapDescriptor:" << sizeof(SwapDescriptor) << "b\n"
              << std::endl;

    RunConfig config;
    if (!config.parse_cmdline(argc, argv))
        return -1;

    stxxl::srandom_number32(config.randomSeed);
    stxxl::set_seed(config.randomSeed);

    benchmark(config);
    std::cout << "Maximum EM allocation: " <<  stxxl::block_manager::get_instance()->get_maximum_allocation() << std::endl;


    return 0;
    /*
	if (argc <= 0)
        return 0;

	std::cout << "Benchmark for ConfigurationModel(Random)..." << std::endl;

	// ConfigurationModel Random

	for (multinode_t num_nodes = 100; num_nodes <= pow(10, runs + 1); num_nodes*= 10) {

        const degree_t max_deg = static_cast<degree_t>(num_nodes / ratio);
        if (max_deg > num_nodes)
            continue;
        if (max_deg < min_deg)
            continue;

		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
		MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

		StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
		hh_gen.generate();

		HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator, TestNodeComparator> cmhh(hh_gen, num_nodes);

		// Start benchmark here
		// Stats-measurements
		stxxl::stats* Stats = stxxl::stats::get_instance();
		stxxl::stats_data stats_begin(*Stats);

		// Run Code here
		cmhh.run();

		std::stringstream fmt;
        fmt << "cm_r" << max_deg << ".log"; 
        std::string fileName = fmt.str();
        std::ofstream cmr_file(fileName);

		// End benchmark
		cmr_file << "min_deg set to: " << min_deg << std::endl;
		cmr_file << "max_deg set to: " << max_deg << std::endl;
        cmr_file << "edges set to: " << cmhh.size() << std::endl;
        cmr_file << "nodes set to: " << num_nodes << std::endl;
		cmr_file << (stxxl::stats_data(*Stats) - stats_begin);

		// count self-loops and multi-edges
        long loops = 0;
        long multi = 0;
        long times = 0;
        
        bool prev_multi = false;

        auto prev_edge = *cmhh;
        if (prev_edge.is_loop())
            ++loops;
        
        ++cmhh;

        for (; !cmhh.empty(); ++cmhh) {
            auto & edge = *cmhh;
            
            // self-loop found
            if (edge.is_loop()) {
                ++loops;
                if (prev_multi)
                    ++times;
                prev_edge = edge;
                prev_multi = false;
                continue;
            }

            // multi-edge found
            if (prev_edge == edge) {
                ++times;
                if (!prev_multi) {
                    ++multi;
                    prev_multi = true;
                }
                prev_edge = edge;
                continue;
            }

            if (prev_multi)
                ++times;
            prev_edge = edge;

            prev_multi = false;
        }

        if (prev_multi)
            ++times;

        cmr_file << "self_loops: " << loops << std::endl;
		cmr_file << "multi_edges: " << multi << std::endl;
        cmr_file << "multi_edges quantities: " << times << std::endl;

		cmhh.clear();

		cmr_file.close();

		}	

// ConfigurationModel RandomRandom

    for (node_t num_nodes = pow(10, start); num_nodes <= pow(10, runs + 1); num_nodes*= 10) {

        const degree_t max_deg = static_cast<degree_t>(num_nodes / ratio);
        if (max_deg > num_nodes)
            continue;
        if (max_deg < min_deg)
            continue;

        HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
        MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

        StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
        hh_gen.generate();

        HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator, TestNodeRandomComparator> cmhh(hh_gen);

        // Start benchmark here
        // Stats-measurements
        stxxl::stats* Stats = stxxl::stats::get_instance();
        stxxl::stats_data stats_begin(*Stats);

        // Run Code here
        cmhh.run();

        std::stringstream fmt;
        fmt << "cm_tupr" << max_deg << ".log"; 
        std::string fileName = fmt.str();
        std::ofstream cmr_file(fileName);

        // End benchmark
        cmr_file << "min_deg set to: " << min_deg << std::endl;
        cmr_file << "max_deg set to: " << max_deg << std::endl;
        cmr_file << "edges set to: " << cmhh.size() << std::endl;
        cmr_file << "nodes set to: " << num_nodes << std::endl;
        cmr_file << (stxxl::stats_data(*Stats) - stats_begin);

        // count self-loops and multi-edges
        long loops = 0;
        long multi = 0;
        long times = 0;
        
        bool prev_multi = false;

        auto prev_edge = *cmhh;
        if (prev_edge.is_loop())
            ++loops;
        
        ++cmhh;

        for (; !cmhh.empty(); ++cmhh) {
            auto & edge = *cmhh;
            
            // self-loop found
            if (edge.is_loop()) {
                ++loops;
                if (prev_multi)
                    ++times;
                prev_edge = edge;
                prev_multi = false;
                continue;
            }

            // multi-edge found
            if (prev_edge == edge) {
                ++times;
                if (!prev_multi) {
                    ++multi;
                    prev_multi = true;
                }
                prev_edge = edge;
                continue;
            }

            if (prev_multi)
                ++times;
            prev_edge = edge;

            prev_multi = false;
        }

        if (prev_multi)
            ++times;

        cmr_file << "self_loops: " << loops << std::endl;
        cmr_file << "multi_edges: " << multi << std::endl;
        cmr_file << "multi_edges quantities: " << times << std::endl;

        cmhh.clear();

        cmr_file.close();

        }   

	return 0;*/
}
