#include <iostream>
#include <chrono>

#include <stxxl/cmdline>

#include <stack>
#include <stxxl/vector>

#include <PowerlawDegreeSequence.h>
#include <HavelHakimiGenerator.h>

#include <DistributionCount.h>
#include <HavelHakimiGeneratorRLE.h>

#include <DegreeDistributionCheck.h>
#include <SwapGenerator.hpp>
#include <EdgeSwapInternalSwaps.hpp>
#include <EdgeSwapTFP.hpp>


struct RunConfig {
    stxxl::uint64 numNodes;
    stxxl::uint64 numEdges;
    stxxl::uint64 minDeg;
    stxxl::uint64 maxDeg;
    double gamma;

    bool swapInternal;
    bool swapTFP;
    stxxl::uint64 swapsPerIteration;
    stxxl::uint64 swapsPerTFPIteration;

    unsigned int randomSeed;

    stxxl::uint64 sweep_min;
    stxxl::uint64 sweep_max;
    stxxl::uint64 sweep_steps_per_dec;


    RunConfig()
          : numNodes(10 * IntScale::M)
          , numEdges(50 * IntScale::M)
          , minDeg(2)
          , maxDeg(100000)
          , gamma(-2.0)
          , swapInternal(false)
          , swapsPerIteration(1024*1024)
          , swapsPerTFPIteration(0)
          , sweep_min(IntScale::K)
          , sweep_max(IntScale::G)
          , sweep_steps_per_dec(4)
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

        cp.add_bytes (CMDLINE_COMP('n', "num-nodes", numNodes, "Generate # nodes, Default: 10 Mi"));
        cp.add_bytes (CMDLINE_COMP('m', "num-edges", numEdges, "If >0 generated sorted edge list is gets truncated"));
        cp.add_bytes (CMDLINE_COMP('a', "min-deg",   minDeg,   "Min. Deg of Powerlaw Deg. Distr."));
        cp.add_bytes (CMDLINE_COMP('b', "max-deg",   maxDeg,   "Max. Deg of Powerlaw Deg. Distr."));
        cp.add_uint  (CMDLINE_COMP('s', "seed",      randomSeed,   "Initial seed for PRNG"));

        cp.add_flag  (CMDLINE_COMP('i', "swap-internal", swapInternal, "Perform edge swaps in internal memory"));
        cp.add_flag  (CMDLINE_COMP('t', "swap-tfp", swapTFP, "Perform edge swaps using TFP"));
        cp.add_bytes (CMDLINE_COMP('p', "swaps-per-iteration", swapsPerIteration, "Number of swaps per iteration"));
        cp.add_bytes (CMDLINE_COMP('q', "swaps-per-tfp-iteration", swapsPerTFPIteration, "Number of swaps per TFP iteration"));

        cp.add_bytes (CMDLINE_COMP('x', "sweep-min", sweep_min, "Min. Number of swaps"));
        cp.add_bytes (CMDLINE_COMP('y', "sweep-max", sweep_max, "Max. Number of swaps"));
        cp.add_bytes (CMDLINE_COMP('z', "sweep-steps", sweep_steps_per_dec, "Number of steps in sweep per decade"));


        if (!cp.process(argc, argv)) {
            cp.print_usage();
            return false;
        }

        cp.print_result();
        return true;
    }
};

template <typename Generator, typename Vector>
void materialize(Generator& gen, Vector & edges, stxxl::stats * stats, stxxl::stats_data& stats_begin) {
    std::cout << "Stats after filling of prio queue:" << (stxxl::stats_data(*stats) - stats_begin);

    edges.resize(gen.maxEdges());
    auto endIt = stxxl::stream::materialize(gen, edges.begin());
    edges.resize(endIt - edges.begin());

    std::cout << "Generated " << edges.size() << " edges of possibly " << gen.maxEdges() << " edges" << std::endl;
}



void benchmark(RunConfig & config) {
    stxxl::stats * stats = stxxl::stats::get_instance();
    stxxl::stats_data stats_begin(*stats);

    PowerlawDegreeSequence degreeSequence(config.minDeg, config.maxDeg, config.gamma, config.numNodes);

    // create edge list
    using result_vector_type = stxxl::VECTOR_GENERATOR<edge_t>::result;
    result_vector_type edges;
    DistributionCount<PowerlawDegreeSequence> dcount(degreeSequence);
    HavelHakimiGeneratorRLE<DistributionCount<PowerlawDegreeSequence>> hhgenerator(dcount);
    materialize(hhgenerator, edges, stats, stats_begin);

    STXXL_VERBOSE0("Edge list generated");

    // sort edge list
    result_vector_type swapEdges(edges.size());
    {
        result_vector_type::bufreader_type edgeReader(edges);
        stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> edgeSorter(GenericComparator<edge_t>::Ascending(), SORTER_MEM);
        while (!edgeReader.empty()) {
            if (edgeReader->first < edgeReader->second) {
                edgeSorter.push(edge_t {edgeReader->first, edgeReader->second});
            } else {
                edgeSorter.push(edge_t {edgeReader->second, edgeReader->first});
            }

            ++edgeReader;
        }

        edgeSorter.sort();

        stxxl::stream::materialize(edgeSorter, swapEdges.begin());
    }

    STXXL_VERBOSE0("Edge list sorted");

    // truncate edge list
    if (config.numEdges) {
        if (config.numEdges > edges.size()) {
            std::cerr << "numEdges > number of edges generated " << edges.size() << std::endl;
            abort();
        }

        edges.resize(config.numEdges);

        STXXL_VERBOSE0("Edge list truncated to " << config.numEdges);
    }

    // generate largest swap vector
    stxxl::VECTOR_GENERATOR<SwapDescriptor>::result swaps_orig(config.sweep_max);
    {
        SwapGenerator swapGen(config.sweep_max, edges.size());
        auto endit =  stxxl::stream::materialize(swapGen, swaps_orig.begin());
        STXXL_UNUSED(endit);
        assert(static_cast<uint_t>(endit - swaps_orig.begin()) == config.sweep_max);
        STXXL_VERBOSE("Swaps generated");
    }

    unsigned int iter = 1;
    for(uint_t num_swaps = config.sweep_min; 
        num_swaps <= config.sweep_max; 
        num_swaps = static_cast<uint_t>(num_swaps * pow(10.0, 1.0 / config.sweep_steps_per_dec))
    ) {
        STXXL_VERBOSE0("Begin iteration " << iter++ << " with |num_swaps|=" << num_swaps);

        stxxl::VECTOR_GENERATOR<SwapDescriptor>::result swaps(swaps_orig);
        swaps.resize(num_swaps);

        STXXL_VERBOSE0("Swap vector updated");

        if (config.swapInternal) {
            result_vector_type medges(swapEdges);
            STXXL_VERBOSE0("Start Internal");
            auto stat_start = stxxl::stats_data(*stats);
            EdgeSwapInternalSwaps internalSwaps(medges, swaps, config.swapsPerIteration);
            internalSwaps.run();
            STXXL_VERBOSE0("Completed Internal Swaps" << (stxxl::stats_data(*stats) - stat_start));
        }

        if (config.swapTFP) {
            result_vector_type medges(swapEdges);
            STXXL_VERBOSE0("Start TFP");
            auto stat_start = stxxl::stats_data(*stats);
            EdgeSwapTFP::EdgeSwapTFP TFPSwaps(medges, swaps);
            //TFPSwaps.setDisplayDebug(true);
            TFPSwaps.run(config.swapsPerTFPIteration);
            STXXL_VERBOSE0("Completed TFP" << (stxxl::stats_data(*stats) - stat_start));
        }
    }
}

int main(int argc, char* argv[]) {
#ifndef NDEBUG
    std::cout << "[build with assertions]" << std::endl;
#endif
    std::cout << STXXL_VERSION_INTEGER << std::endl;
    RunConfig config;
    if (!config.parse_cmdline(argc, argv))
        return -1;

    stxxl::srandom_number32(config.randomSeed);
    stxxl::set_seed(config.randomSeed);

    benchmark(config);
    return 0;
}
