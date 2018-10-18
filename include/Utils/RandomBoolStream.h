#pragma once
#include <defs.h>
#include <random>

class RandomBoolStream {
private:
    unsigned int _flag_bits_remaining = 0;
    uint64_t _flag_bits;

    std::mt19937_64 _rand_gen;


public:
    RandomBoolStream(seed_t seed)
          : _rand_gen(seed)
    {
       operator++();
    }

    bool empty() const { return false; }

    bool operator *() const { return _flag_bits & 1; }

    RandomBoolStream& operator++() {
        if (UNLIKELY(!_flag_bits_remaining)) {
            _flag_bits_remaining = 8 * sizeof(_flag_bits);
            _flag_bits = _rand_gen();
        } else {
            _flag_bits >>= 1;
            --_flag_bits_remaining;
        }

        return *this;
    }
};
