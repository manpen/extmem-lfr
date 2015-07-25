#pragma once

#include <stxxl/bits/common/types.h>


template <typename T>
struct DistributionBlockDescriptor {
    T value;
    stxxl::uint64 count;
    stxxl::uint64 index;
    using value_type = T;
};


template <typename T>
struct DistributionEqualComparator
{
    bool operator () (const T& a, const T& b) const { return a == b; }
};

/* 
 * This stream object performs a "run-lenght encoding", i.e. it counts the number of
 * equal items in the input stream. Each block is collapsed into one output items containing
 * the value of the input item, the number of items in the block (count) as well as the number
 * of sampled input items so far (index).
 * Assuming the input is sorted (asc or desc) the output is a distribution count.
 */
template <typename InputStream, typename T=stxxl::uint64, typename Comparator=DistributionEqualComparator<T>>
class DistributionCount {
public:    
    using value_type = DistributionBlockDescriptor<T>;
    
private:
    InputStream & _in_stream;
    stxxl::uint64 _items_sampled;
    Comparator _equal;
    
    value_type _current_element;
    bool _empty;
    
    void _sample_next_block() {
        if (_in_stream.empty()) {
            _empty = true;
            return;
        }
         
        T value = *_in_stream;
        stxxl::uint64 count = 0;
        
        while( !_in_stream.empty() && _equal(*_in_stream, value) ) {
            ++_in_stream;
            ++count;
        }
        
        _items_sampled += count;
        
        _current_element = {
            .value = value, .count = count, .index = _items_sampled
        };
    }
    
public:
    DistributionCount(InputStream& input)
        : _in_stream(input)
        , _items_sampled(0)
        , _current_element({0,0,0}) // prevent irrelevant warnings further downstream
        , _empty(false)
    {
        _sample_next_block();
    }
    
    const value_type & operator * () const {
        return _current_element;
    };
    
    const value_type * operator -> () const { 
        return &_current_element;
    };
    
    DistributionCount & operator++ () {
        _sample_next_block();
        return *this;
    }
        
    bool empty() const { 
        return _empty;
    };
};

template <typename T>
inline std::ostream &operator<<(std::ostream &os, DistributionBlockDescriptor<T> const &m) {
    return os << "{value: " << m.value << ", degree:" << m.count << ", index: " << m.index << "}";
}
