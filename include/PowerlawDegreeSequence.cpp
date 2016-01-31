/**
 * @file
 * @brief Parameterizable Power-Law Distribution as STXXL stream
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */
#include <cmath>
#include "PowerlawDegreeSequence.h"

PowerlawDegreeSequence::PowerlawDegreeSequence(int_t minDeg, int_t maxDeg, double gamma, int_t numNodes)
        : minDeg(minDeg), maxDeg(maxDeg), gamma(gamma), randomStream(numNodes), sortedRandomStream(randomStream, rand_less(), SORTER_MEM) {

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
