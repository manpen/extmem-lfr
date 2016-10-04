#pragma once

#include <defs.h>

class UniformRandomStream {
public:
    using value_type = double;

private:
    uint_t _counter;

    STDRandomEngine _rand_gen;
    std::uniform_real_distribution<value_type> _rand_distr{0.0, 1.0};

    value_type _current;

public:
    UniformRandomStream(uint_t elements, seed_t seed)
        : _counter(elements+1), _rand_gen(seed)
    {}

    const value_type& operator * () const {
        return _current;
    };

    UniformRandomStream & operator++ () {
        _current = _rand_distr(_rand_gen);
        --_counter;
        return *this;
    };

    bool empty() const {
        return _counter == 0;
    };
};
