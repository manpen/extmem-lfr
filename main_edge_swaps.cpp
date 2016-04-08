#include <iostream>
#include <chrono>

#include <string>
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
//#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/EdgeSwapTFP.h>
//#include <EdgeSwaps/IMEdgeSwap.h>

enum EdgeSwapAlgo {
    IM,
    SEMI, // InternalSwaps
    TFP,
    PTFP
};

struct RunConfig {
    stxxl::uint64 numNodes;
    stxxl::uint64 minDeg;
    stxxl::uint64 maxDeg;
    double gamma;

    stxxl::uint64 numSwaps;
    stxxl::uint64 runSize;
    stxxl::uint64 batchSize;

    unsigned int randomSeed;

    EdgeSwapAlgo edgeSwapAlgo;

    bool verbose;

    RunConfig() 
        : numNodes(10 * IntScale::Mi)
        , minDeg(2)
        , maxDeg(100000)
        , gamma(-2.0)

        , numSwaps(numNodes)
        , runSize(numNodes/10)
        , batchSize(IntScale::Mi)

        , verbose(false)
    {
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
            cp.add_bytes (CMDLINE_COMP('n', "num-nodes", numNodes, "Generate # nodes, Default: 10 Mi"));
            cp.add_bytes (CMDLINE_COMP('a', "min-deg",   minDeg,   "Min. Deg of Powerlaw Deg. Distr."));
            cp.add_bytes (CMDLINE_COMP('b', "max-deg",   maxDeg,   "Max. Deg of Powerlaw Deg. Distr."));
            cp.add_double(CMDLINE_COMP('g', "gamma",     gamma,    "Gamma of Powerlaw Deg. Distr."));
            cp.add_uint  (CMDLINE_COMP('s', "seed",      randomSeed,   "Initial seed for PRNG"));

            cp.add_bytes  (CMDLINE_COMP('m', "num-swaps", numSwaps,   "Number of swaps to perform"));
            cp.add_bytes  (CMDLINE_COMP('r', "run-size", runSize, "Number of swaps per graph scan"));
            cp.add_bytes  (CMDLINE_COMP('k', "batch-size", batchSize, "Batch size of PTFP"));

            cp.add_string(CMDLINE_COMP('e', "swap-algo", swap_algo_name, "SwapAlgo to use: IM, SEMI, TFP, PTFP (default)"));

            cp.add_flag(CMDLINE_COMP('v', "verbose", verbose, "Include debug information selectable at runtime"));
            
            if (!cp.process(argc, argv)) {
                cp.print_usage();
                return false;
            }
        }

        // select edge swap algo
        {
            std::transform(swap_algo_name.begin(), swap_algo_name.end(), swap_algo_name.begin(), ::toupper);

            if      (swap_algo_name.empty() ||
                     0 == swap_algo_name.compare("PTFP")) { edgeSwapAlgo = PTFP; }
            else if (0 == swap_algo_name.compare("TFP"))  { edgeSwapAlgo = TFP; }
            else if (0 == swap_algo_name.compare("SEMI")) { edgeSwapAlgo = SEMI; }
            else if (0 == swap_algo_name.compare("IM"))   { edgeSwapAlgo = IM; }
            else {
                std::cerr << "Invalid edge swap algorithm specified: " << swap_algo_name << std::endl;
                cp.print_usage();
                return false;
            }
            std::cout << "Using edge swap algo: " << swap_algo_name << std::endl;
        }

        if (runSize > std::numeric_limits<swapid_t>::max()) {
           std::cerr << "RunSize is limited by swapid_t. Max: " << std::numeric_limits<swapid_t>::max() << std::endl;
           return false;
        }


        cp.print_result();
        return true;
    }
};





void benchmark(RunConfig & config) {
    stxxl::stats *stats = stxxl::stats::get_instance();
    stxxl::stats_data stats_begin(*stats);

    // Build edge list
    using edge_vector_t = stxxl::VECTOR_GENERATOR<edge_t>::result;
    edge_vector_t edges;

    bool use_edge_stream = (config.edgeSwapAlgo == TFP);
    EdgeStream edge_stream;
    {
        IOStatistics hh_report("HHEdges");

        // prepare generator
        HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
        MonotonicPowerlawRandomStream<false> degreeSequence(config.minDeg, config.maxDeg, config.gamma, config.numNodes);
        StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
        hh_gen.generate();

        if (use_edge_stream) {
            StreamPusher<decltype(hh_gen), EdgeStream>(hh_gen, edge_stream);
            edge_stream.consume();
        } else {
            // materialize stream
            std::cout << "Up to " << hh_gen.maxEdges() << " edges expected" << std::endl;
            edges.resize(hh_gen.maxEdges());
            auto endIt = stxxl::stream::materialize(hh_gen, edges.begin());
            edges.resize(endIt - edges.begin());
        }

    }
    edgeid_t number_of_edges = use_edge_stream ? edge_stream.size() : edges.size();
    std::cout << "Generated " << number_of_edges << " edges\n";

    // Build swaps
    SwapGenerator swap_gen(config.numSwaps, number_of_edges);
    using swap_vector_t = stxxl::VECTOR_GENERATOR<SwapDescriptor>::result;
    swap_vector_t swaps;
    if (config.edgeSwapAlgo == TFP) {
        IOStatistics swap_report("SwapGenerator");
        swaps.resize(config.numSwaps);

        auto endIt = stxxl::stream::materialize(swap_gen, swaps.begin());
        assert(static_cast<size_t>(endIt - swaps.begin()) == config.numSwaps);

    } else {
        std::cout << "Swap Algo accepts swaps as stream\n";
    }

    // Perform edge swaps
    {

        IOStatistics swap_report("SwapStats");
        switch (config.edgeSwapAlgo) {
/*            case IM: {
                IMEdgeSwap swap_algo(edges);
                StreamPusher<decltype(swap_gen), decltype(swap_algo)>(swap_gen, swap_algo);
                swap_algo.run();
                break;
            }

            case SEMI: {
                EdgeSwapInternalSwaps swap_algo(edges, config.runSize);
                StreamPusher<decltype(swap_gen), decltype(swap_algo)>(swap_gen, swap_algo);
                swap_algo.run();
                break;
            } */

            case TFP: {
                EdgeSwapTFP::EdgeSwapTFP TFPSwaps(edge_stream, swaps);
                if (config.verbose) TFPSwaps.setDisplayDebug(true);
                TFPSwaps.run(config.runSize);
                break;
            }

            case PTFP: {
                EdgeSwapParallelTFP::EdgeSwapParallelTFP swap_algo(edges, config.runSize);
                StreamPusher<decltype(swap_gen), decltype(swap_algo)>(swap_gen, swap_algo);
                swap_algo.run();
                break;
            }
        }
    }
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

    return 0;
}
