//
// Created by michael on 08.07.15.
//

#include <cmath>
#include "PowerlawDegreeSequence.h"

PowerlawDegreeSequence::PowerlawDegreeSequence(stxxl::int64 minDeg, stxxl::int64 maxDeg, double gamma, stxxl::int64 numNodes)
        : minDeg(minDeg), maxDeg(maxDeg), gamma(gamma), randomStream(numNodes), sortedRandomStream(randomStream, rand_less(), 64*1024*1024) {

    probabilitySum = 0;
    for (double d = minDeg; d <= maxDeg; ++d) {
        probabilitySum += std::pow(d, gamma);
    }

    currentDegree = minDeg;
    nextThreshold = std::pow(currentDegree, gamma) / probabilitySum;
    findNextDegree();
}

void PowerlawDegreeSequence::findNextDegree() {
    while (currentDegree < maxDeg && nextThreshold < *sortedRandomStream) {
        ++currentDegree;
        nextThreshold += (std::pow(currentDegree, gamma) / probabilitySum);
    }
}

PowerlawDegreeSequence &PowerlawDegreeSequence::operator++() {
    ++sortedRandomStream;
    
    if (!empty())
        findNextDegree();
    
    return *this;
}
