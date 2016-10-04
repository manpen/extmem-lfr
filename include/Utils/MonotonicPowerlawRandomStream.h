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
        double scale;

        double exponent;
    };


protected:
    MonotonicUniformRandomStream<true> _uniform_random;

    const degree_t _min_degree;
    const degree_t _max_degree;
    const double _gamma;
    const double _scale;

    const double _normalization;


    value_type _current;
    value_type _current_scaled;
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
        _current_scaled = static_cast<value_type>(_current * _scale);
    }

public:
    MonotonicPowerlawRandomStream(int_t minDegree, int_t maxDegree, double gamma, int_t numberOfNodes, double scale = 1.0, seed_t seed = stxxl::get_next_seed())
        : _uniform_random(numberOfNodes, seed)
        , _min_degree(minDegree)
        , _max_degree(maxDegree)
        , _gamma(gamma)
        , _scale(scale)
        , _normalization(_compute_normalization())
        , _current(Increasing ? _min_degree : _max_degree)
        , _current_weight(std::pow(double(_current), _gamma) / _normalization)
    {
        assert(minDegree > 0);
        assert(minDegree < maxDegree);
        assert(numberOfNodes > 1);
        assert(scale > 0);

        _update();
    }

    MonotonicPowerlawRandomStream(const Parameters& p, seed_t seed = stxxl::get_next_seed()) :
        MonotonicPowerlawRandomStream(p.minDegree, p.maxDegree, p.exponent, p.numberOfNodes, p.scale, seed)
    {}

    bool empty() const {
        return _uniform_random.empty();
    }

    const value_type& operator*() const {
        return _current_scaled;
    }

    MonotonicPowerlawRandomStream&operator++() {
        ++_uniform_random;
        if (LIKELY(!empty()))
            _update();
        return *this;
    }
};
