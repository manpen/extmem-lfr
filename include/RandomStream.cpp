//
// Created by michael on 08.07.15.
//

#include "RandomStream.h"

RandomStream::RandomStream(stxxl::int64 elements) : counter(elements), current(gen()) { }

