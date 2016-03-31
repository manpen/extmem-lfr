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

    const degree_t _min_degree;
    const degree_t _max_degree;
    const double _gamma;

    const double _normalization;


    value_type _current;
    double _current_weight;

    double _compute_normalization() {
        double sum = 0.0;
        for(degree_t d = _min_degree; d <= _max_degree; d++)
            sum += std::pow((double)d, _gamma);
        return sum;
    }

    void _update() {
        while(_current_weight <= *_uniform_random) {
            if (Increasing) {
                _current++;
                assert(_current <= _max_degree);
            } else {
                _current--;
                assert(_current >= _min_degree);
            }

            _current_weight += std::pow(double(_current), _gamma) / _normalization;
        }
    }

public:
    MonotonicPowerlawRandomStream(int_t minDegree, int_t maxDegree, double gamma, int_t numberOfNodes)
        : _uniform_random(numberOfNodes)
        , _min_degree(minDegree)
        , _max_degree(maxDegree)
        , _gamma(gamma)
        , _normalization(_compute_normalization())
        , _current(Increasing ? _min_degree : _max_degree)
        , _current_weight(std::pow(double(_current), _gamma) / _normalization)
    {
        assert(minDegree > 0);
        assert(minDegree < maxDegree);
        assert(numberOfNodes > 1);

        _update();
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
        if (LIKELY(!empty()))
            _update();
        return *this;
    }
};
