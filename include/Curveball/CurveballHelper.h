/*
 * CurveballHelper.h
 *
 *      Author:  Manuel Penschuck
 */
#pragma once

#ifndef CB_CURVEBALLHELPER_H
#define CB_CURVEBALLHELPER_H

#include <algorithm>
#include <cassert>
#include <iterator>

namespace CurveballImpl {

template<typename It, typename RandomBits>
void random_partition(It begin, It end, size_t left_part, RandomBits& urng) {
    const size_t setsize = std::distance(begin, end);
    const size_t right_part = setsize - left_part;

    if (!left_part || !right_part) return;
    assert(left_part < setsize);

    auto two_random = [&urng] (size_t a, size_t b) {
        auto x = std::uniform_int_distribution<size_t>{0, (a * b) - 1}(urng);
        return std::make_pair(x / a, x % b);
    };

    using std::swap; // allow ADL

    if (left_part < right_part) {
        auto it = begin;
        for(size_t i = 1; i < left_part; i+=2) {
            auto rand = two_random(setsize-i+1, setsize - i);
            swap(*it++, *(begin + (setsize - 1 - rand.first)));
            swap(*it++, *(begin + (setsize - 1 - rand.second)));
        }

        if (left_part % 2) {
            auto x = std::uniform_int_distribution<size_t>{left_part-1, setsize-1}(urng);
            swap(*it, *(begin+x));
        }

    } else {
        auto it = begin + (setsize - 1);

        for (size_t i = setsize - 2; i >= left_part; i -= 2) {
            auto rand = two_random(i+1, i);
            swap(*it--, *(begin + rand.first) );
            swap(*it--, *(begin + rand.second) );
        }

        if (right_part % 2) {
            auto x = std::uniform_int_distribution<size_t>{0, right_part}(urng);
            swap(*it--, *(begin+x));
        }
    }
};


}

#endif
