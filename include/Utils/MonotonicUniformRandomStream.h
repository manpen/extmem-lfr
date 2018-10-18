#pragma once

#include <random>
#include <defs.h>
#include <stxxl/bits/common/utils.h>
#include <stxxl/bits/common/seed.h>

template <bool Increasing = true>
class MonotonicUniformRandomStream {
public:
    using value_type = double;

private:
    using T = double; // internal floating point rep

    STDRandomEngine _rand_gen;
    std::uniform_real_distribution<double> _rand_distr;

    uint_t _elements_left;
    bool _empty;
    value_type _current;

public:
    MonotonicUniformRandomStream(uint_t elements, seed_t seed = stxxl::get_next_seed())
        : _rand_gen(seed)
        , _rand_distr(0, 1.0)
        , _elements_left(elements)
        , _empty(!elements)
        , _current(Increasing ? 0.0 : 1.0)
    {++(*this);}

    MonotonicUniformRandomStream& operator++() {
        assert(!_empty);
        if (UNLIKELY(!_elements_left)) {
            _empty = true;
        } else {
            const double rand = _rand_distr(_rand_gen);

            if (Increasing) {
                _current = T(1.0) - (1.0 - _current) * std::pow(T(1.0) - rand, 1.0 / T(_elements_left));
            } else {
                _current *= std::pow(T(1.0) - rand, 1.0 / T(_elements_left));
            }
            _elements_left--;
        }

        return *this;
    }

    const value_type& operator * () const {
        return _current;
    };

    bool empty() const {
        return _empty;
    };
};
