#pragma once
#include <defs.h>
#include <stxxl/random>
#include <memory>

class RandomBoolStream {
private:
    unsigned int _flag_bits_remaining = 0;
    uint64_t _flag_bits;
    std::unique_ptr<stxxl::random_number64> _random_integer_value_ptr;
    stxxl::random_number64& _random_integer;
public:
    RandomBoolStream() : _random_integer_value_ptr(new stxxl::random_number64), _random_integer(*_random_integer_value_ptr) { operator++(); }
    RandomBoolStream(stxxl::random_number64& generator) : _random_integer(generator) { operator++(); }

    bool empty() const { return false; }

    bool operator *() const { return _flag_bits & 1; }

    RandomBoolStream& operator++() {
        if (!_flag_bits_remaining) {
            _flag_bits_remaining = 8 * sizeof(_flag_bits);
            _flag_bits = _random_integer();
        } else {
            _flag_bits >>= 1;
            --_flag_bits_remaining;
        }

        return *this;
    }
};
