#include <CluewebReader.h>

#ifdef BOOST_SUPPORT
#include <iostream>
#include <fstream>
#include <sstream>
#include <bits/stream_iterator.h>
#include <Utils/StableAssert.h>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#endif

EdgeStream read_clueweb_file(std::string & path) {
    EdgeStream result;
#ifdef BOOST_SUPPORT
    const uint64_t max_node = 733019372 + 245388725;

    // Open the file
    boost::iostreams::file_source zippped_input (path, std::ios_base::in | std::ios_base::binary);

    // Uncompress the file with the BZ2 library and its Boost wrapper
    boost::iostreams::filtering_istream bunzip2Filter;
    bunzip2Filter.push (boost::iostreams::bzip2_decompressor());
    bunzip2Filter.push (zippped_input);

    std::string str;

    std::istringstream is;
    uint64_t number_edges;
    {
        // read first line -> number of edges
        std::getline(bunzip2Filter, str);
        is.str(str);
        is >> number_edges;
    }
    std::cout << "Expect " << number_edges << " edges" << std::endl;


    for (uint64_t vertex_id=0; vertex_id < max_node && std::getline(bunzip2Filter, str); vertex_id++) {
        std::istringstream in(str);

        for(std::istream_iterator<uint64_t> it(in); it != std::istream_iterator<uint64_t>(); ++it) {
            if (*it > max_node) continue;
            STABLE_ASSERT_LS(vertex_id, *it);
            result.push({vertex_id, *it});
        }

        if (vertex_id && 0 == (vertex_id % 10000000))
            std::cout << "Processed vertex " << vertex_id << "; produced " << result.size() << " edges (" << (100.0 * result.size() / number_edges) << "%)"  << std::endl;
    }

    std::cout << "Produced " << result.size() << " edges; expected " << number_edges << std::endl;
#else
    std::cerr << "Build without Boost-Support. Cannot read Clueweb file" << std::endl;
    abort();

#endif
    return result;
}
