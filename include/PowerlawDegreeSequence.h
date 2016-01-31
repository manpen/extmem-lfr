/**
 * @file
 * @brief Parameterizable Power-Law Distribution as STXXL stream
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */
#pragma once

#include <stxxl/bits/common/types.h>
#include "RandomStream.h"
#include <functional>
#include <stxxl/bits/stream/sort_stream.h>

#include <defs.h>

class PowerlawDegreeSequence {
private:
    struct rand_less : std::less<double> {
        double min_value() const { return -0.1; };
        double max_value() const { return 1.1; };
    };

    int_t minDeg;
    int_t maxDeg;
    double gamma;
    double probabilitySum;
    int_t currentDegree;
    double nextThreshold;

    RandomStream randomStream;
    using SortedRandomStream = stxxl::stream::sort<RandomStream, rand_less>;
    SortedRandomStream sortedRandomStream;

    void findNextDegree();

public:
    struct Parameters {
        int_t minDegree;
        int_t maxDegree;
        int_t numberOfNodes;
        double exponent;
    };

    using value_type = int_t ;

    PowerlawDegreeSequence(int_t minDeg, int_t maxDeg, double gamma, int_t numNodes);
    PowerlawDegreeSequence(const Parameters& p) : PowerlawDegreeSequence(p.minDegree, p.maxDegree, p.exponent, p.numberOfNodes) {}
    const int_t & operator * () const { return currentDegree; };
    const int_t * operator -> () const { return &currentDegree; };
    PowerlawDegreeSequence & operator++ () ;
    bool empty() const { return sortedRandomStream.empty(); };
};
