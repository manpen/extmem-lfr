#include <iostream>
#include <chrono>

#include <stxxl/cmdline>
#include <stxxl/vector>

#include "PowerlawDegreeSequence.h"
#include "HavelHakimiGenerator.h"

#include "DistributionCount.h"
#include "HavelHakimiGeneratorRLE.h"

#include "DegreeDistributionCheck.h"


struct RunConfig {
    stxxl::uint64 numNodes;
    stxxl::uint64 minDeg;
    stxxl::uint64 maxDeg;
    double gamma;

    bool rle;
    bool showInitDegrees;
    bool showResDegrees;

    unsigned int randomSeed;

    RunConfig() 
        : numNodes(10 * 1024 * 1024)
        , minDeg(2)
        , maxDeg(100000)
        , gamma(-2.0)
        , rle(false)
        , showInitDegrees(false)
        , showResDegrees(false)
    {
        using myclock = std::chrono::high_resolution_clock;
        myclock::duration d = myclock::now() - myclock::time_point::min();
        randomSeed = d.count();
    }


    bool parse_cmdline(int argc, char* argv[]) {
        stxxl::cmdline_parser cp;

        cp.add_bytes ('n', "num-nodes", numNodes, "Generate # nodes, Default: 10 Mi");
        cp.add_bytes ('a', "min-deg",   minDeg,   "Min. Deg of Powerlaw Deg. Distr.");
        cp.add_bytes ('b', "max-deg",   maxDeg,   "Max. Deg of Powerlaw Deg. Distr.");
        cp.add_double('g', "gamma",     gamma,    "Gamma of Powerlaw Deg. Distr.");
        cp.add_uint  ('s', "seed",      randomSeed,   "Initial seed for PRNG");
        
        cp.add_flag  ('r', "rle",       rle,          "Use RLE HavelHakimi");
        cp.add_flag  ('i', "init-degrees", showInitDegrees, "Output requested degrees (no HH gen)");
        cp.add_flag  ('d', "res-degrees",  showResDegrees,  "Output degree distribution of result");

        if (!cp.process(argc, argv)) {
            cp.print_usage();
            return false;
        }

        cp.print_result();
        return true;
    }
};


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
    typedef stxxl::VECTOR_GENERATOR<HavelHakimiGenerator::value_type>::result result_vector_type;
    result_vector_type edges;
    
    if (config.rle) {
        DistributionCount<PowerlawDegreeSequence> dcount(degreeSequence);
        HavelHakimiGeneratorRLE<DistributionCount<PowerlawDegreeSequence>> hhgenerator(dcount);
        std::cout << "Stats after filling of prio queue:" << (stxxl::stats_data(*stats) - stats_begin);
        
        edges.resize(hhgenerator.maxEdges());
        auto endIt = stxxl::stream::materialize(hhgenerator, edges.begin());
        edges.resize(endIt - edges.begin());

        std::cout << "Generated " << edges.size() << " edges of possibly " << hhgenerator.maxEdges() << " edges" << std::endl;
        
    } else {
        HavelHakimiGenerator hhgenerator(degreeSequence);
        std::cout << "Stats after filling of prio queue:" << (stxxl::stats_data(*stats) - stats_begin);
        
        edges.resize(hhgenerator.maxEdges());
        auto endIt = stxxl::stream::materialize(hhgenerator, edges.begin());
        edges.resize(endIt - edges.begin());
        std::cout << "Generated " << edges.size() << " edges of possibly " << hhgenerator.maxEdges() << " edges" << std::endl;
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
    }
}



int main(int argc, char* argv[]) {
   RunConfig config;
   if (!config.parse_cmdline(argc, argv))
      return -1;
   
   stxxl::srandom_number32(config.randomSeed);
   
   benchmark(config);
   return 0;
}
