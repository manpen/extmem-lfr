//
// Created by michael on 08.07.15.
//

#ifndef MYPROJECT_RANDOMSTREAM_H
#define MYPROJECT_RANDOMSTREAM_H


#include <stxxl/bits/common/types.h>
#include <stxxl/bits/common/rand.h>

class RandomStream {
private:
    stxxl::int64 counter;
    stxxl::random_uniform_fast gen;
    double current;
public:
    typedef double value_type;
    RandomStream(stxxl::int64 elements);
    const double & operator * () const { return current; };
    const double * operator -> () const { return &current; };
    RandomStream & operator++ () {
        current = gen();
        --counter;
        return *this;
    };
    bool empty() { return counter == 0; };
};


#endif //MYPROJECT_RANDOMSTREAM_H
