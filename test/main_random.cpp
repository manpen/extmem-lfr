#include <iostream>
#include <string>
#include <chrono>

#include <stxxl/cmdline>

#include <defs.h>
#include <Utils/FloatDistributionCount.h>
#include <Utils/UniformRandomStream.h>
#include <Utils/MonotonicUniformRandomStream.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <Utils/RandomBoolStream.h>

struct RunConfig {
    stxxl::uint64 randomPoints;
    stxxl::uint64  histogramBins;

    stxxl::uint64  minValue;
    stxxl::uint64  maxValue;
    double gamma;

    seed_t randomSeed;
    std::string test;

    RunConfig() :
        randomPoints(1000000)
        , histogramBins(10000)

        , minValue(10)
        , maxValue(10000)
        , gamma(-2)
        , test("hist-powerlaw")
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

        cp.add_bytes (CMDLINE_COMP('n', "num-points", randomPoints,  "Number of random numbers"));
        cp.add_bytes (CMDLINE_COMP('b', "num-bin",    histogramBins, "Number of histogram bins"));

        cp.add_bytes (CMDLINE_COMP('x', "min",    minValue, "Value's lower bound"));
        cp.add_bytes (CMDLINE_COMP('y', "max",    maxValue, "Value's upper bound"));
        cp.add_double(CMDLINE_COMP('z', "gamma",  gamma,    "Exponent of distribution"));

        cp.add_uint  (CMDLINE_COMP('s', "seed",      randomSeed,   "Initial seed for PRNG"));

        cp.add_param_string("test", test, "Test to execute");

        cp.set_verbose_process(false);

        if (!cp.process(argc, argv)) {
            cp.print_usage();
            return false;
        }

        stxxl::srandom_number32(randomSeed);
        stxxl::set_seed(randomSeed);

        return true;
    }
};

void test_hist_unif(RunConfig& conf) {
    UniformRandomStream rs(conf.randomPoints, conf.randomSeed);
    FloatDistributionCount histogram(-0.1, 1.1, conf.histogramBins);
    histogram.consume(rs);
    histogram.dump();
}

void test_hist_mono_unif(RunConfig& conf) {
    MonotonicUniformRandomStream<> rs(conf.randomPoints, conf.randomSeed);
    FloatDistributionCount histogram(-0.1, 1.1, conf.histogramBins);
    histogram.consume(rs);
    histogram.dump();
}


void test_hist_powerlaw(RunConfig& conf) {
    MonotonicPowerlawRandomStream<> rs(conf.minValue, conf.maxValue, conf.gamma, conf.randomPoints, 1.0, conf.randomSeed);
    FloatDistributionCount histogram(conf.minValue/2, conf.maxValue*2, conf.maxValue*2-conf.minValue/2+1);
    histogram.consume(rs);
    histogram.dump();
}

void count_powerlaw(RunConfig& conf) {
    MonotonicPowerlawRandomStream<> rs(conf.minValue, conf.maxValue, conf.gamma, conf.randomPoints, 1.0, conf.randomSeed);

    uint64_t count = 0;
    degree_t prev_degree = 0;

    while(!rs.empty()) {
        count += (prev_degree != *rs);
        prev_degree = *rs;
        ++rs;
    }

    std::cout << conf.minValue << " "
              << conf.maxValue << " "
              << conf.randomPoints << " "
              << conf.gamma << " "
              << count << std::endl;
}



int main(int argc, char* argv[]) {
   RunConfig config;
   if (!config.parse_cmdline(argc, argv)) abort();


   if (!config.test.compare("hist_unif")) {test_hist_unif(config);}
    else if (!config.test.compare("hist_mono_unif")) {test_hist_mono_unif(config);}
    else if (!config.test.compare("hist_powerlaw")) {test_hist_powerlaw(config);}
    else if (!config.test.compare("count_powerlaw")) {count_powerlaw(config);}
    else {
        std::cerr << "Unknown test" << std::endl;
        abort();
    }

    return 0;
}
