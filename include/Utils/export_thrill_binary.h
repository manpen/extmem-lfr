#pragma once

#include <EdgeStream.h>
#include <fstream>
#include <stxxl/sorter>
#include <defs.h>
#include <GenericComparator.h>
#include <stack>

/*!
 * CRTP class to enhance item/memory writer classes with Varint encoding and
 * String encoding.
 */
    //! Append a varint to the writer.
template <typename stream>
stream& PutVarint(stream& s, uint64_t v) {
	if (v < 128) {
		s << uint8_t(v);
	}
	else if (v < 128 * 128) {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t((v >> 07) & 0x7F);
	}
	else if (v < 128 * 128 * 128) {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t(((v >> 07) & 0x7F) | 0x80);
		s << uint8_t((v >> 14) & 0x7F);
	}
	else if (v < 128 * 128 * 128 * 128) {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t(((v >> 07) & 0x7F) | 0x80);
		s << uint8_t(((v >> 14) & 0x7F) | 0x80);
		s << uint8_t((v >> 21) & 0x7F);
	}
	else if (v < 128llu * 128 * 128 * 128 * 128) {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t(((v >> 07) & 0x7F) | 0x80);
		s << uint8_t(((v >> 14) & 0x7F) | 0x80);
		s << uint8_t(((v >> 21) & 0x7F) | 0x80);
		s << uint8_t((v >> 28) & 0x7F);
	}
	else if (v < 128llu * 128 * 128 * 128 * 128 * 128) {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t(((v >> 07) & 0x7F) | 0x80);
		s << uint8_t(((v >> 14) & 0x7F) | 0x80);
		s << uint8_t(((v >> 21) & 0x7F) | 0x80);
		s << uint8_t(((v >> 28) & 0x7F) | 0x80);
		s << uint8_t((v >> 35) & 0x7F);
	}
	else if (v < 128llu * 128 * 128 * 128 * 128 * 128 * 128) {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t(((v >> 07) & 0x7F) | 0x80);
		s << uint8_t(((v >> 14) & 0x7F) | 0x80);
		s << uint8_t(((v >> 21) & 0x7F) | 0x80);
		s << uint8_t(((v >> 28) & 0x7F) | 0x80);
		s << uint8_t(((v >> 35) & 0x7F) | 0x80);
		s << uint8_t((v >> 42) & 0x7F);
	}
	else if (v < 128llu * 128 * 128 * 128 * 128 * 128 * 128 * 128) {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t(((v >> 07) & 0x7F) | 0x80);
		s << uint8_t(((v >> 14) & 0x7F) | 0x80);
		s << uint8_t(((v >> 21) & 0x7F) | 0x80);
		s << uint8_t(((v >> 28) & 0x7F) | 0x80);
		s << uint8_t(((v >> 35) & 0x7F) | 0x80);
		s << uint8_t(((v >> 42) & 0x7F) | 0x80);
		s << uint8_t((v >> 49) & 0x7F);
	}
	else if (v < 128llu * 128 * 128 * 128 * 128 * 128 * 128 * 128 * 128) {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t(((v >> 07) & 0x7F) | 0x80);
		s << uint8_t(((v >> 14) & 0x7F) | 0x80);
		s << uint8_t(((v >> 21) & 0x7F) | 0x80);
		s << uint8_t(((v >> 28) & 0x7F) | 0x80);
		s << uint8_t(((v >> 35) & 0x7F) | 0x80);
		s << uint8_t(((v >> 42) & 0x7F) | 0x80);
		s << uint8_t(((v >> 49) & 0x7F) | 0x80);
		s << uint8_t((v >> 56) & 0x7F);
	}
	else {
		s << uint8_t(((v >> 00) & 0x7F) | 0x80);
		s << uint8_t(((v >> 07) & 0x7F) | 0x80);
		s << uint8_t(((v >> 14) & 0x7F) | 0x80);
		s << uint8_t(((v >> 21) & 0x7F) | 0x80);
		s << uint8_t(((v >> 28) & 0x7F) | 0x80);
		s << uint8_t(((v >> 35) & 0x7F) | 0x80);
		s << uint8_t(((v >> 42) & 0x7F) | 0x80);
		s << uint8_t(((v >> 49) & 0x7F) | 0x80);
		s << uint8_t(((v >> 56) & 0x7F) | 0x80);
		s << uint8_t((v >> 63) & 0x7F);
	}

	return s;
};

void export_as_thrill_binary(EdgeStream &edges, node_t num_nodes, const std::string& filename) {
	edges.rewind();

	std::ofstream out_stream(filename, std::ios::trunc | std::ios::binary);
	//out_stream << num_nodes << " " << num_edges << " " << 0 << std::endl;
	
	std::vector<node_t> neighbors;
	for (node_t u = 0; u < num_nodes; ++u) {
		neighbors.clear();
		for (; !edges.empty() && edges->first == u; ++edges) {
			neighbors.push_back(edges->second);
		}
		PutVarint(out_stream, neighbors.size());

		for (node_t v : neighbors) {
			static_assert(std::is_same<int32_t, node_t>::value, "Node type is not int32 anymore, adjust code!");
			out_stream.write(reinterpret_cast<const char*>(&v), 4);
		}
	}

	out_stream.close();
};
