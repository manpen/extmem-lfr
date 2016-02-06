/**
 * @file
 * @brief Parameterizable Power-Law Distribution as STXXL stream
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */
#pragma once

#include <stxxl/stream>
#include <defs.h>
#include <cmath>
#include "MonotonicUniformRandomStream.h"

template <bool Increasing = true>
class MonotonicPowerlawRandomStream {
public:
    using value_type = degree_t;

    struct Parameters {
        int_t minDegree;
        int_t maxDegree;
        int_t numberOfNodes;

        double exponent;
    };


protected:
    MonotonicUniformRandomStream<Increasing> _uniform_random;
    const double _offset;
    const double _scale;
    const double _exp;

    value_type _current;

    void _compute() {
        _current = std::round(std::pow(*_uniform_random * _scale + _offset, _exp) + 0.5);
    }

public:
    MonotonicPowerlawRandomStream(int_t minDegree, int_t maxDegree, double gamma, int_t numberOfNodes)
        : _uniform_random(numberOfNodes)
        , _offset(std::pow(double(minDegree), 1.0+gamma))
        , _scale (std::pow(double(maxDegree), 1.0+gamma)-_offset)
        , _exp(1.0/(1.0+gamma))
    {
        assert(std::abs(gamma) > std::numeric_limits<double>::epsilon());
        assert(minDegree < maxDegree);
        assert(numberOfNodes > 1);
        _compute();
    }

    MonotonicPowerlawRandomStream(const Parameters& p) :
        MonotonicPowerlawRandomStream(p.minDegree, p.maxDegree, p.exponent, p.numberOfNodes)
    {}

    bool empty() const {
        return _uniform_random.empty();
    }

    const value_type& operator*() const {
        return _current;
    }

    MonotonicPowerlawRandomStream&operator++() {
        ++_uniform_random;
        _compute();
        return *this;
    }
};
