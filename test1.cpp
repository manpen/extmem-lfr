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
#include "SwapGenerator.h"
#include "EdgeSwapInternalSwaps.h"
#include "EdgeSwapTFP.h"


struct RunConfig {
    stxxl::uint64 numNodes;
    stxxl::uint64 minDeg;
    stxxl::uint64 maxDeg;
    double gamma;

    bool rle;
    bool showInitDegrees;
    bool showResDegrees;
    bool internalMem;
    bool swapInternal;
    bool swapTFP;
    stxxl::uint64 swapsPerIteration;
    stxxl::uint64 numSwaps;

    unsigned int randomSeed;

    RunConfig() 
        : numNodes(10 * 1024 * 1024)
        , minDeg(2)
        , maxDeg(100000)
        , gamma(-2.0)
        , rle(false)
        , showInitDegrees(false)
        , showResDegrees(false)
        , internalMem(false)
        , swapInternal(false)
        , swapsPerIteration(1024*1024)
        , numSwaps(numNodes)
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
        cp.add_bytes (CMDLINE_COMP('a', "min-deg",   minDeg,   "Min. Deg of Powerlaw Deg. Distr."));
        cp.add_bytes (CMDLINE_COMP('b', "max-deg",   maxDeg,   "Max. Deg of Powerlaw Deg. Distr."));
        //cp.add_double(CMDLINE_COMP('g', "gamma",     gamma,    "Gamma of Powerlaw Deg. Distr."));
        cp.add_uint  (CMDLINE_COMP('s', "seed",      randomSeed,   "Initial seed for PRNG"));

        cp.add_flag  (CMDLINE_COMP('t', "internal-mem", internalMem,     "Use Internal Memory for HH prio/stack (rather than STXXL containers)"));
        cp.add_flag  (CMDLINE_COMP('r', "rle",          rle,             "Use RLE HavelHakimi"));
        cp.add_flag  (CMDLINE_COMP('i', "init-degrees", showInitDegrees, "Output requested degrees (no HH gen)"));
        cp.add_flag  (CMDLINE_COMP('d', "res-degrees",  showResDegrees,  "Output degree distribution of result"));

        cp.add_flag  (CMDLINE_COMP('m', "swap-internal", swapInternal, "Perform edge swaps in internal memory"));
        cp.add_flag  (CMDLINE_COMP('e', "swap-tfp", swapTFP, "Perform edge swaps using TFP"));
        cp.add_bytes  (CMDLINE_COMP('p', "swaps-per-iteration", swapsPerIteration, "Number of swaps per iteration"));
        cp.add_bytes  (CMDLINE_COMP('z', "num-swaps", numSwaps,   "Number of random swaps"));

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

    if (config.showInitDegrees) {
        DistributionCount<PowerlawDegreeSequence> dcount(degreeSequence);
        
        for(; !dcount.empty(); ++dcount) {
            auto block = *dcount;
            std::cout << block.value << " " << block.count << " # InitDD  Degree Count" << std::endl;
        }
        
        return;
    }
    
    // create vector
    using result_vector_type = stxxl::VECTOR_GENERATOR<edge_t>::result;
    result_vector_type edges;
    
    if (config.rle) {
        DistributionCount<PowerlawDegreeSequence> dcount(degreeSequence);
        HavelHakimiGeneratorRLE<DistributionCount<PowerlawDegreeSequence>> hhgenerator(dcount);
        materialize(hhgenerator, edges, stats, stats_begin);
        
    } else {
        if (config.internalMem) {
            HavelHakimiPrioQueueInt prio_queue;
            std::stack<HavelHakimiNodeDegree> stack;
            
            HavelHakimiGenerator<HavelHakimiPrioQueueInt, std::stack<HavelHakimiNodeDegree>> hhgenerator{prio_queue, stack, degreeSequence};
            materialize(hhgenerator, edges, stats, stats_begin);
        } else {
            using hh_prio_queue = HavelHakimiPrioQueueExt<16 * IntScale::Mi, IntScale::Mi>;
            hh_prio_queue prio_queue;
            
            using hh_stack = stxxl::STACK_GENERATOR<HavelHakimiNodeDegree, stxxl::external, stxxl::grow_shrink>::result;
            hh_stack stack;
            
            HavelHakimiGenerator<hh_prio_queue, hh_stack> hhgenerator{prio_queue, stack, degreeSequence};
            materialize(hhgenerator, edges, stats, stats_begin);
        }            
    }

    if (config.swapInternal || config.swapTFP) {
        int_t m = edges.size();

        result_vector_type swapEdges(m);
        {
            result_vector_type::bufreader_type edgeReader(edges);
            stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending> edgeSorter(GenericComparator<edge_t>::Ascending(), 128*IntScale::Mi);
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

        SwapGenerator swapGen(config.numSwaps, m);
        stxxl::VECTOR_GENERATOR<SwapDescriptor>::result swaps(config.numSwaps);
        auto endIt = stxxl::stream::materialize(swapGen, swaps.begin());
        if (endIt - swaps.begin() != static_cast<int_t>(swaps.size())) {
            throw std::runtime_error("Error, the number of generated swaps is not as specified");
        }

        if (config.swapInternal) {
            auto stat_start = stxxl::stats_data(*stats);
            EdgeSwapInternalSwaps internalSwaps(swapEdges, swaps, config.swapsPerIteration);
            internalSwaps.run();
            std::cout << (stxxl::stats_data(*stats) - stat_start) << std::endl;
        }

        if (config.swapTFP) {
            auto stat_start = stxxl::stats_data(*stats);
            EdgeSwapTFP::EdgeSwapTFP TFPSwaps(swapEdges, swaps);
            TFPSwaps.run(config.swapsPerIteration);
            std::cout << (stxxl::stats_data(*stats) - stat_start) << std::endl;
        }
    }
    
    std::cout << (stxxl::stats_data(*stats) - stats_begin);
    
#if 0
    unsigned int i = 0;
    for (auto edge : edges) {
        std::cout << i++ << ": " << edge.first << ", " << edge.second << std::endl;
    }
#endif

    if (config.showResDegrees) {
        DegreeDistributionCheck<result_vector_type::const_iterator> ddc {edges.begin(), edges.end()};
        
        auto deg_distr = ddc.getDistribution();

        for(; !deg_distr.empty(); ++deg_distr) {
            auto block = *deg_distr;
            std::cout << block.value << " " << block.count << " # ResDD  Degree Count" << std::endl;
        }

        std::cout << "Stats after degree comp:" << (stxxl::stats_data(*stats) - stats_begin);
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
