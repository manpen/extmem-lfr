//
// Created by michael on 08.07.15.
//

#ifndef MYPROJECT_POWERLAWDEGREESEQUENCE_H
#define MYPROJECT_POWERLAWDEGREESEQUENCE_H


#include <stxxl/bits/common/types.h>
#include "RandomStream.h"
#include <functional>
#include <stxxl/bits/stream/sort_stream.h>

class PowerlawDegreeSequence {
private:
    struct rand_less : std::less<double> {
        double min_value() const { return -0.1; };
        double max_value() const { return 1.1; };
    };

    stxxl::int64 minDeg;
    stxxl::int64 maxDeg;
    double gamma;
    double probabilitySum;
    stxxl::int64 currentDegree;
    double nextThreshold;

    RandomStream randomStream;
    typedef stxxl::stream::sort<RandomStream, rand_less> SortedRandomStream;
    SortedRandomStream sortedRandomStream;

    void findNextDegree();

public:
    typedef stxxl::int64 value_type;
    PowerlawDegreeSequence(stxxl::int64 minDeg, stxxl::int64 maxDeg, double gamma, stxxl::int64 numNodes);
    const stxxl::int64 & operator * () const { return currentDegree; };
    const stxxl::int64 * operator -> () const { return &currentDegree; };
    PowerlawDegreeSequence & operator++ () ;
    bool empty() const { return sortedRandomStream.empty(); };
};


#endif //MYPROJECT_POWERLAWDEGREESEQUENCE_H
