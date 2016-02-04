#pragma once

#include <stxxl/bits/common/rand.h>

class UniformRandomStream {
public:
    using value_type = double;

private:
    uint_t _counter;

    stxxl::random_uniform_fast _gen;
    value_type _current;

public:
    UniformRandomStream(uint_t elements)
        : _counter(elements), _current(_gen())
    {}

    const value_type& operator * () const {
        return _current;
    };

    UniformRandomStream & operator++ () {
        _current = _gen();
        --_counter;
        return *this;
    };

    bool empty() const {
        return _counter == 0;
    };
};
