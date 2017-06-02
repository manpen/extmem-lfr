//
// Created by hungt on 28.12.16.
//

#include <iostream>
#include <chrono>

#include <algorithm>
#include <locale>

#include <string>
#include <list>

#include <stxxl/cmdline>

#include <stack>
#include <stxxl/vector>
#include <EdgeStream.h>

#include <Utils/IOStatistics.h>
#include <Utils/ScopedTimer.hpp>

#include <Utils/MonotonicPowerlawRandomStream.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <Utils/StreamPusher.h>
#include <Utils/EdgeToEdgeSwapPusher.h>


#include <DegreeDistributionCheck.h>
#include "SwapGenerator.h"

#include <EdgeSwaps/EdgeSwapTFP.h>

#include <ConfigurationModel/ConfigurationModelRandom.h>
#include <SwapStream.h>
#include <EdgeSwaps/ModifiedEdgeSwapTFP.h>
#include <Utils/export_metis.h>

struct RunConfig {
    stxxl::uint64 numNodes;
    stxxl::uint64 minDeg;
    stxxl::uint64 maxDeg;
    double gamma;
    double scaleDegree;

    enum InputMethod {
        HH,
        CMES,
        FILE,
        FILE_CMES
    };

    InputMethod inputMethod;
    std::string inputFile;

    std::string snapFiles;


    stxxl::uint64 numSwaps;
    stxxl::uint64 runSize;
    stxxl::uint64 batchSize;

    stxxl::uint64 internalMem;


    unsigned int randomSeed;
    unsigned int degreeDistrSeed;


    bool verbose;

    double factorNoSwaps;
    unsigned int noRuns;


    std::string snapshotsAt;

    unsigned int edgeSizeFactor;

    double randomSwapsInCMES;

    RunConfig()
            : numNodes(10 * IntScale::Mi)
            , minDeg(2)
            , maxDeg(100000)
            , gamma(2.0)
            , scaleDegree(1.0)

            , inputMethod(HH)
            , snapFiles("snapshot%p.bin")

            , numSwaps(0)
            , runSize(numNodes/10)
            , batchSize(IntScale::Mi)
            , internalMem(8 * IntScale::Gi)

            , verbose(false)
            , factorNoSwaps(1)
            , noRuns(8)
            , edgeSizeFactor(1)
            , randomSwapsInCMES(0)
    {
        using myclock = std::chrono::high_resolution_clock;
        myclock::duration d = myclock::now() - myclock::time_point::min();
        randomSeed = d.count();
        degreeDistrSeed = 123456789 * randomSeed;
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
        bool input_hh = false;
        bool input_cm = false;
        bool input_file = false;


        // setup and gather parameters
        {
            cp.add_bytes (CMDLINE_COMP('n', "num-nodes",    numNodes,        "Generate # nodes, Default: 10 Mi"));
            cp.add_bytes (CMDLINE_COMP('a', "min-deg",      minDeg,          "Min. Deg of Powerlaw Deg. Distr."));
            cp.add_bytes (CMDLINE_COMP('b', "max-deg",      maxDeg,          "Max. Deg of Powerlaw Deg. Distr."));
            cp.add_double(CMDLINE_COMP('g', "gamma",        gamma,           "Minus Gamma of Powerlaw Deg. Distr.; default: 2"));
            cp.add_double(CMDLINE_COMP('d', "scale-degree", scaleDegree,     "ScaleDegree of PWL-Distr"));
            cp.add_uint  (CMDLINE_COMP('s', "seed",         randomSeed,      "Initial seed for PRNG"));
            cp.add_uint  (CMDLINE_COMP('S', "degree-seed",  degreeDistrSeed, "Initial seed for PRNG of degree distr"));

            cp.add_bytes (CMDLINE_COMP('m', "num-swaps", numSwaps,   "Number of swaps to perform"));
            cp.add_bytes (CMDLINE_COMP('r', "run-size", runSize, "Number of swaps per graph scan"));
            cp.add_bytes (CMDLINE_COMP('k', "batch-size", batchSize, "Batch size of PTFP"));

            cp.add_bytes (CMDLINE_COMP('i', "ram", internalMem, "Internal memory"));

            cp.add_flag  (CMDLINE_COMP('v', "verbose", verbose, "Include debug information selectable at runtime"));

            cp.add_double(CMDLINE_COMP('x', "factor-swaps",     factorNoSwaps,    "Overwrite -m = noEdges * x"));
            cp.add_uint  (CMDLINE_COMP('y', "no-runs",      noRuns,   "Overwrite r = m / y  + 1"));

            cp.add_flag  (CMDLINE_COMP('H', "input-hh",    input_hh,          "use Havel Hakimi; default"));
            cp.add_flag  (CMDLINE_COMP('c', "input-cm",    input_cm,          "use Configuration Model + Rewiring"));
            cp.add_double(CMDLINE_COMP('C', "cmes-random", randomSwapsInCMES, "Include X*|E| random swaps during CMES rewiring steps; default: 0"));

            cp.add_string(CMDLINE_COMP('A', "snapshots-at", snapshotsAt, "comma-sep list of phases, start:stop:step as in python allows"));

            cp.add_string(CMDLINE_COMP('I', "input-file", inputFile, "read edge list from file"));
            cp.add_string(CMDLINE_COMP('o', "snap-files", snapFiles, "path to snapshot files; %p is replace by number of phases"));


            if (!cp.process(argc, argv)) {
                cp.print_usage();
                return false;
            }
        }

        // select input stage
        {
            input_file = !inputFile.empty();

            if (input_hh && input_cm) {
                std::cerr << "Can enable either HH or CMES; not both" << std::endl;
                return false;
            }

            if (input_hh && input_file) {
                std::cerr << "Can enable either HH or File; not both" << std::endl;
                return false;
            }

            if (input_cm && !input_file) {inputMethod = CMES;}
            else if (input_cm && input_file) {inputMethod = FILE_CMES;}
            else if (input_file) {inputMethod = FILE;}
            else {inputMethod = HH;}
        }


        if (runSize > std::numeric_limits<swapid_t>::max()) {
            std::cerr << "RunSize is limited by swapid_t. Max: " << std::numeric_limits<swapid_t>::max() << std::endl;
            return false;
        }

        if (scaleDegree * minDeg < 1.0) {
            std::cerr << "Scaling the minimum degree must yield at least 1.0" << std::endl;
            return false;
        }

        if (gamma <= 1.0) {
            std::cerr << "Gamma has to be at least 1.0" << std::endl;
            return false;
        }

        cp.print_result();
        return true;
    }

    std::string snapshotFile(unsigned int phase) {
        std::string result(snapFiles);
        std::string phaseStr = std::to_string(phase);

        size_t index = 0;
        while (true) {
            index = result.find("%p", index);
            if (index == std::string::npos)
                break;

            result.replace(index, 2, phaseStr);
            index += phaseStr.length();
        }

        return result;
    }

    std::list<uint_t> extractPhases(uint_t maxPhase) const {
        std::string str = snapshotsAt;
        str.erase(remove_if(str.begin(), str.end(), isspace), str.end());

        size_t blockBegin = 0;
        size_t blockEnd = 0;

        std::list<uint_t> result;

        while(true) {
            // find next block
            blockBegin = blockEnd;
            blockEnd = str.find(',', blockBegin+1);
            std::string block = str.substr(blockBegin, blockEnd-blockBegin);

            // check if we have a range
            const size_t firstColon = block.find(':');
            if (firstColon == std::string::npos) {
               if (!block.empty())
                  result.push_back(atoll(block.c_str()));

            } else {
                const size_t secondColon = block.find(':', firstColon+1);


                // extract block infos
                uint_t start = firstColon ?  atoll(block.substr(0, firstColon).c_str()) : 0;
                uint_t step = (secondColon == std::string::npos)
                              ? 1
                              : atoll(block.substr(secondColon+1, std::string::npos).c_str());

                uint_t stop;
                if (firstColon+1 == secondColon) {
                    stop = maxPhase;
                } else {
                    if (secondColon == std::string::npos) {
                        if (firstColon+1 == block.size()) {
                            stop = maxPhase;
                        } else {
                            stop = atoll(block.substr(firstColon + 1, std::string::npos).c_str());
                        }
                    } else {
                        stop = atoll(block.substr(firstColon + 1, secondColon - firstColon).c_str());
                    }
                }

                for(uint_t i=start; i < stop; i += step)
                    result.push_back(i);
            }

            // terminate if done
            if (!(blockEnd < str.size()))
                break;

            blockEnd++;
        }

        result.sort();
        result.unique();

        return result;
    }
};


void benchmark(RunConfig & config) {
    stxxl::stats *stats = stxxl::stats::get_instance();
    stxxl::stats_data stats_begin(*stats);

    // Load or generate edge list
    EdgeStream edge_stream;
    {
        switch(config.inputMethod) {
            case RunConfig::InputMethod::HH: {
                std::cout << "Graph input: Havel Hakimi" << std::endl;
                IOStatistics hh_report("HHEdges");

                // prepare generator
                HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
                MonotonicPowerlawRandomStream<false> degreeSequence(config.minDeg, config.maxDeg, -1.0 * config.gamma, config.numNodes, config.scaleDegree, config.degreeDistrSeed);
                StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
                hh_gen.generate();

                StreamPusher<decltype(hh_gen), EdgeStream>(hh_gen, edge_stream);
                edge_stream.consume();

            }
            break;
            case RunConfig::InputMethod::CMES: {
                std::cout << "Graph input: CMES" << std::endl;
                IOStatistics hh_report("CMEM");

                // prepare generator
                HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
                MonotonicPowerlawRandomStream<false> degreeSequence(config.minDeg, config.maxDeg, -1.0 * config.gamma, config.numNodes, config.scaleDegree, config.degreeDistrSeed);
                StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
                hh_gen.generate();

                ConfigurationModelRandom<HavelHakimiIMGenerator> cmhh_gen(hh_gen);
                {
                   ScopedTimer timer("CM");
                   cmhh_gen.run();
                }

                {
                    IOStatistics swap_report("ES for CM");

                    ModifiedEdgeSwapTFP::ModifiedEdgeSwapTFP init_algo(edge_stream, config.runSize, config.numNodes,
                                                                       config.internalMem);

                    EdgeToEdgeSwapPusher<decltype(cmhh_gen), EdgeStream, ModifiedEdgeSwapTFP::ModifiedEdgeSwapTFP>
                            cm_to_emes_pusher(cmhh_gen, edge_stream, init_algo);
                    edge_stream.consume();


                    const edgeid_t min_swaps = edge_stream.size() * config.randomSwapsInCMES;


                    unsigned int iteration = 0;
                    while (init_algo.runnable()) {
                        std::cout << "[CM-ES] Remove illegal edges: Iteration " << ++iteration << std::endl;
                        std::cout << "Graph contains " << edge_stream.size() << " edges\n"
                                     "  " << edge_stream.selfloops() << " selfloops\n"
                                     "  " << edge_stream.multiedges() << " multiedges"
                        << std::endl;

                        std::cout << "Swaps pending: " << init_algo.swaps_pushed() << std::endl;

                        if (init_algo.swaps_pushed() < min_swaps * 0.75) {
                            const swapid_t additional_swaps = min_swaps - init_algo.swaps_pushed();

                            SwapGenerator swap_gen(additional_swaps, edge_stream.size());
                            StreamPusher<decltype(swap_gen), decltype(init_algo)>pusher (swap_gen, init_algo);

                            std::cout << "Added additional swaps: " << additional_swaps << std::endl;
                        }


                        {
                            ScopedTimer timer("Rewiring run");
                            init_algo.run();
                        }
                    }

                    std::cout << "[CM-ES] Number of iterations: " << iteration << std::endl;
                }
            }
            break;
            case RunConfig::InputMethod::FILE: {
                IOStatistics read_report("Read");
                stxxl::linuxaio_file file(config.inputFile, stxxl::file::DIRECT | stxxl::file::RDONLY);
                stxxl::vector<edge_t> vector(&file);
                typename decltype(vector)::bufreader_type reader(vector);

                for(; !reader.empty(); ++reader)
                    edge_stream.push(*reader);

                edge_stream.consume();
            }
            break;
            default:
                std::cerr << "Unhandled input stage" << std::endl;
                abort();
        }


    }

    std::cout << "Graph contains " << edge_stream.size() << " edges\n"
                 "  " << edge_stream.selfloops() << " selfloops\n"
                 "  " << edge_stream.multiedges() << " multiedges"
    << std::endl;

    if (config.factorNoSwaps > 0) {
        config.numSwaps = edge_stream.size() * config.factorNoSwaps;
        std::cout << "Set numSwaps = " << config.numSwaps << std::endl;
    }

    if (config.noRuns > 0) {
        config.runSize = config.numSwaps / config.noRuns + 1;
        std::cout << "Set runSize = " << config.runSize << std::endl;
    }




    // Randomize with EM-ES
    {
        auto snapshotPhases =
                config.extractPhases((config.numSwaps + config.runSize - 1) / config.runSize);

        // report phases
        {
            std::cout << "[Snapshots] Requested snapshots for phases:";
            if (snapshotPhases.empty()) {
                std::cout << "None";
            } else {
                for (const auto &i : snapshotPhases)
                    std::cout << " " << i;
            }
            std::cout << "\n";
        }

        auto writeSnapshots = [&edge_stream, &config, &snapshotPhases] (uint_t iteration) {
            std::cout << "Callback for iteration " << iteration << std::endl;

            while (!snapshotPhases.empty() && snapshotPhases.front() < iteration)
                snapshotPhases.pop_front();

            if (snapshotPhases.empty())
                return;

            if (snapshotPhases.front() == iteration) {
                std::cout << "[Export] Store snapshot to " << config.snapshotFile(iteration) << std::endl;
                export_as_thrillbin_sorted(edge_stream, config.snapshotFile(iteration), config.numNodes);
                edge_stream.rewind();
            }
        };

        if (!config.numSwaps) {
            writeSnapshots(0);

        } else  {
            SwapGenerator swap_gen(config.numSwaps, edge_stream.size());

            EdgeSwapTFP::EdgeSwapTFP swap_algo(edge_stream, config.runSize, config.numNodes, config.internalMem, writeSnapshots);

            {
                IOStatistics swap_report("Randomization");
                StreamPusher<decltype(swap_gen), decltype(swap_algo)>(swap_gen, swap_algo);
                swap_algo.run();
            }
        }
    }

    std::cout << "Final graph contains " << edge_stream.size() << " edges\n"
                 "  " << edge_stream.selfloops() << " selfloops\n"
                 "  " << edge_stream.multiedges() << " multiedges"
    << std::endl;
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
    std::cout <<
        "int_t:         " << sizeof(int_t) << "b\n"
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

    {
        const auto max_alloc = stxxl::block_manager::get_instance()->get_maximum_allocation();
        std::cout << "Maximum EM allocation: "
            << max_alloc << "b (" <<  ((max_alloc + (1<<20) - 1) / (1<<20)) << " Mb)"
            << std::endl;
    }

    return 0;
}
