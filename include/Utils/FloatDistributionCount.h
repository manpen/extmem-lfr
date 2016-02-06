#pragma once
#include <iostream>
#include <vector>
#include <assert.h>

class FloatDistributionCount {
    std::vector<uint_t> _counts;
    uint_t _total_counts;
    const long double _left;
    const long double _right;
    const long double _bin_size;

public:
    FloatDistributionCount(double left, double right, uint_t bins) :
        _counts(bins), _total_counts(0),
        _left(left), _right(right), _bin_size( (right-left) / bins )
    {
        assert(left < right);
        assert(bins > 0);
    }

    void add(double x) {
        if (_left <= x && _right >= x) {
            _counts[  (x-_left)/_bin_size ]++;
            _total_counts++;
        }
    }

    template <typename Stream>
    void consume(Stream& stream) {
        for(; !stream.empty(); ++stream)
            add(*stream);
    }

    uint_t operator[] (uint_t bin) const {
        return _counts.at(bin);
    }

    void dump(bool commul = false, bool show_zero_counts = false) const {
        uint64_t val = 0;
        for(unsigned int i=0; i<_counts.size(); i++) {
            if (show_zero_counts || _counts[i]) {
                val = (commul ? (val+_counts[i]) : _counts[i]);

                std::cout << i << " "
                << (_bin_size * i + _left) << " "
                << (_bin_size * (i+1) + _left) << " "
                << ((double)val / _total_counts) << " "
                << val << "\n";
            }
        }
    }
};
