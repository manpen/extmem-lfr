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
    const bool _gamma_is_one;

    const double _offset;
    const double _scale;
    const double _exp;

    value_type _current;

    void _compute() {
        if (_gamma_is_one) {
            _current = std::round(std::exp(*_uniform_random * _scale + _offset));
        } else {
            _current = std::round(std::pow(*_uniform_random * _scale + _offset, _exp) + 0.5);
        }
    }

public:
    MonotonicPowerlawRandomStream(int_t minDegree, int_t maxDegree, double gamma, int_t numberOfNodes)
        : _uniform_random(numberOfNodes)
        , _gamma_is_one(fabs(gamma + 1) <= std::numeric_limits<double>::epsilon())
        , _offset(_gamma_is_one ? (std::log(minDegree)) : std::pow(double(minDegree), 1.0+gamma))
        , _scale (_gamma_is_one ? (std::log((double)maxDegree/minDegree)) : (std::pow(double(maxDegree), 1.0+gamma)-_offset))
        , _exp(_gamma_is_one ? 1 : 1.0/(1.0+gamma))
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
