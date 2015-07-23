#include <iostream>

#include <stxxl/cmdline>

#include <stxxl/vector>
#include "PowerlawDegreeSequence.h"
#include "HavelHakimiGenerator.h"

struct RunConfig {
   stxxl::uint64 numNodes;
   stxxl::uint64 minDeg;
   stxxl::uint64 maxDeg;
   double gamma;
   
   RunConfig() 
      : numNodes(10 * 1024 * 1024)
      , minDeg(2)
      , maxDeg(100000)
      , gamma(-2.0)
   {} 
   
   bool parse_cmdline(int argc, char* argv[]) {
      stxxl::cmdline_parser cp;
      cp.add_opt_param_bytes("num-nodes", numNodes, "Generate # nodes, Default: 10 Mi");
      cp.add_opt_param_bytes("min-deg", minDeg, "Min. Deg of Powerlaw Deg. Distr.");
      cp.add_opt_param_bytes("max-deg", maxDeg, "Max. Deg of Powerlaw Deg. Distr.");
      cp.add_opt_param_double("gamma", gamma, "Gamma of Powerlaw Deg. Distr.");
      

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

    HavelHakimiGenerator hhgenerator(degreeSequence);

    std::cout << "Stats after filling of prio queue:" << (stxxl::stats_data(*stats) - stats_begin);
    
    // create vector
    typedef stxxl::VECTOR_GENERATOR<HavelHakimiGenerator::value_type>::result result_vector_type;
    result_vector_type vector(hhgenerator.maxEdges());

    auto endIt = stxxl::stream::materialize(hhgenerator, vector.begin());
    vector.resize(endIt - vector.begin());

    std::cout << "Generated " << vector.size() << " edges of possibly " << hhgenerator.maxEdges() << " edges" << std::endl;

    std::cout << (stxxl::stats_data(*stats) - stats_begin);
    
    
#if 0
    for (auto edge : vector) {
        std::cout << edge.first << ", " << edge.second << std::endl;
    }
#endif
}



int main(int argc, char* argv[]) {
   RunConfig config;
   if (!config.parse_cmdline(argc, argv))
      return -1;
   
   benchmark(config);
   return 0;
}
