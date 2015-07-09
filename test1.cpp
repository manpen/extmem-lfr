#include <iostream>

#include <stxxl/vector>
#include "PowerlawDegreeSequence.h"
#include "HavelHakimiGenerator.h"

int main() {

    stxxl::int64 numNodes = 10 * 1024 * 1024;
    PowerlawDegreeSequence degreeSequence(2, 100000, -2, numNodes);

    HavelHakimiGenerator hhgenerator(degreeSequence);

    // create vector
    typedef stxxl::VECTOR_GENERATOR<HavelHakimiGenerator::value_type>::result result_vector_type;
    result_vector_type vector(hhgenerator.maxEdges());

    auto endIt = stxxl::stream::materialize(hhgenerator, vector.begin());
    vector.resize(endIt - vector.begin());

    std::cout << "Generated " << vector.size() << " edges of possibly " << hhgenerator.maxEdges() << " edges" << std::endl;

#if 0
    for (auto edge : vector) {
        std::cout << edge.first << ", " << edge.second << std::endl;
    }
#endif

    return 0;
}
