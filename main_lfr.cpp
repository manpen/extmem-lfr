#include <iostream>
#include <chrono>

#include <stxxl/cmdline>

#include <defs.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <LFR/LFR.h>
#include <LFR/LFRCommunityAssignBenchmark.h>
#include <Utils/export_metis.h>

class RunConfig {
    void _update_structs() {
        node_distribution_param.exponent = node_gamma;
        node_distribution_param.minDegree = node_min_degree;
        node_distribution_param.maxDegree = node_max_degree;
        node_distribution_param.numberOfNodes = number_of_nodes;

        community_distribution_param.exponent = community_gamma;
        community_distribution_param.minDegree = community_min_members;
        community_distribution_param.maxDegree = community_max_members;
        community_distribution_param.numberOfNodes = number_of_communities;
    }

public:
    stxxl::uint64 number_of_nodes;
    stxxl::uint64 number_of_communities;

    stxxl::uint64 node_min_degree;
    stxxl::uint64 node_max_degree;
    stxxl::uint64 max_degree_within_community;
    double node_gamma;

    stxxl::uint64 overlap_degree;
    stxxl::uint64 overlapping_nodes;

    stxxl::uint64 community_min_members;
    stxxl::uint64 community_max_members;

    double community_gamma;

    double mixing;
    stxxl::uint64 max_bytes;
    unsigned int randomSeed;

    std::string output_filename, partition_filename;

    MonotonicPowerlawRandomStream<false>::Parameters node_distribution_param;
    MonotonicPowerlawRandomStream<false>::Parameters community_distribution_param;

    unsigned int lfr_bench_rounds;
    bool lfr_bench_comassign;
    bool lfr_bench_comassign_retry;

    RunConfig() :
        number_of_nodes      (100000),
        number_of_communities( 10000),
        node_min_degree(10),
        node_max_degree(number_of_nodes/10),
        max_degree_within_community(node_max_degree),
        node_gamma(-2.0),
        overlap_degree(0),
        overlapping_nodes(0),
        community_min_members(  25),
        community_max_members(1000),
        community_gamma(-2.0),
        mixing(0.5),
        max_bytes(10*UIntScale::Gi),
        lfr_bench_rounds(100),
        lfr_bench_comassign(false),
        lfr_bench_comassign_retry(false)
    {
        using myclock = std::chrono::high_resolution_clock;
        myclock::duration d = myclock::now() - myclock::time_point::min();
        randomSeed = d.count();
        _update_structs();
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

        cp.add_bytes (CMDLINE_COMP('n', "num-nodes", number_of_nodes, "Number of nodes"));
        cp.add_bytes (CMDLINE_COMP('c', "num-communities", number_of_communities, "Number of communities"));

        cp.add_bytes (CMDLINE_COMP('i', "node-min-degree",   node_min_degree,   "Minumum node degree"));
        cp.add_bytes (CMDLINE_COMP('a', "node-max-degree",   node_max_degree,   "Maximum node degree"));
        cp.add_double(CMDLINE_COMP('j', "node-gamma",        node_gamma,        "Exponent of node degree distribution"));

        cp.add_bytes (CMDLINE_COMP('x', "community-min-members",   community_min_members,   "Minumum community size"));
        cp.add_bytes (CMDLINE_COMP('y', "community-max-members",   community_max_members,   "Maximum community size"));
        cp.add_double(CMDLINE_COMP('z', "community-gamma",         community_gamma,         "Exponent of community size distribution"));

        cp.add_uint  (CMDLINE_COMP('s', "seed",      randomSeed,   "Initial seed for PRNG"));

        cp.add_bytes (CMDLINE_COMP('l', "num-overlap-node",   overlapping_nodes,   "Minumum node degree"));
        cp.add_bytes (CMDLINE_COMP('k', "overlap-members",   overlap_degree,   "Maximum node degree"));

        cp.add_double(CMDLINE_COMP('m', "mixing",        mixing,         "Fraction node edge being inter-community"));
        cp.add_bytes(CMDLINE_COMP('b', "max-bytes", max_bytes, "Maximum number of bytes of main memory to use"));

        cp.add_string(CMDLINE_COMP('o', "output", output_filename, "Output filename; the generated graph will be written as METIS graph"));
        cp.add_string(CMDLINE_COMP('p', "partition-output", partition_filename, "Partition output filename; every line contains a node and the communities of the node separated by spaces"));

        cp.add_uint(CMDLINE_COMP('d', "lfr-bench-rounds", lfr_bench_rounds, "# of rounds for LFR benchmarks"));
        cp.add_flag(CMDLINE_COMP('e', "lfr-comassign", lfr_bench_comassign, "Perform LFR comassign benchmark"));
        cp.add_flag(CMDLINE_COMP('f', "lfr-comassign-retry", lfr_bench_comassign_retry, "Perform LFR comassign retry benchmark"));

        assert(number_of_communities < std::numeric_limits<community_t>::max());

        if (!cp.process(argc, argv)) {
            cp.print_usage();
            return false;
        }

        if (overlapping_nodes> number_of_nodes) {
            std::cerr << "Number of overlapping exceed total number of nodes" << std::endl;
            return false;
        }

        cp.print_result();

        _update_structs();

        return true;
    }
};

int main(int argc, char* argv[]) {
#ifndef NDEBUG
    std::cout << "[build with assertions]" << std::endl;
#endif

    omp_set_nested(1);

    RunConfig config;
    if (!config.parse_cmdline(argc, argv))
        return -1;

    stxxl::srandom_number32(config.randomSeed);
    stxxl::set_seed(config.randomSeed);

    LFR::LFR lfr(config.node_distribution_param,
                 config.community_distribution_param,
                 config.mixing,
                 config.max_bytes);

    LFR::OverlapConfig oconfig;
    oconfig.constDegree.multiCommunityDegree = config.overlap_degree;
    oconfig.constDegree.overlappingNodes = config.overlapping_nodes;

    lfr.setOverlap(LFR::OverlapMethod::constDegree, oconfig);

    if (config.lfr_bench_comassign) {
        LFR::LFRCommunityAssignBenchmark bench(lfr);
        bench.computeDistribution(config.lfr_bench_rounds);
    } else if(config.lfr_bench_comassign_retry) {
        LFR::LFRCommunityAssignBenchmark bench(lfr);
        bench.computeRetryRate(config.lfr_bench_rounds);
    } else {
        lfr.run();

        if (!config.output_filename.empty()) {
            export_as_metis(lfr.get_edges(), config.node_distribution_param.numberOfNodes, config.output_filename);
        }

        if (!config.partition_filename.empty()) {
            std::ofstream output_stream(config.partition_filename, std::ios::trunc);
            lfr.export_community_assignment(output_stream);
            output_stream.close();
        }
    }

    std::cout << "Maximum EM allocation: " <<  stxxl::block_manager::get_instance()->get_total_allocation() << std::endl;    

    return 0;
}
