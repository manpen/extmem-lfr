#pragma once
#include <LFR/LFR.h>

namespace LFR {
    class LFRCommunityAssignBenchmark {
        LFR &_lfr;

    public:
        LFRCommunityAssignBenchmark(LFR &lfr) : _lfr(lfr) { }

        void computeDistribution(unsigned int rounds);
    };
};