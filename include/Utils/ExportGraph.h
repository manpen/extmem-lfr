#pragma once

#include <EdgeStream.h>
#include <fstream>
#include <stxxl/sorter>
#include <defs.h>
#include <GenericComparator.h>

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


void export_as_metis(EdgeStream &edges, node_t num_nodes, const std::string& filename) {
	edges.rewind();
	edgeid_t num_edges = edges.size();

	std::ofstream out_stream(filename, std::ios::trunc);
	out_stream << num_nodes << " " << num_edges << " " << 0 << std::endl;

	using EdgeComparator = typename GenericComparator<edge_t>::Ascending;

	stxxl::sorter<edge_t, EdgeComparator> edge_sorter(EdgeComparator(), SORTER_MEM);
	for (; !edges.empty(); ++edges) {
		edge_sorter.push(edge_t(edges->second, edges->first));
	}
	edge_sorter.sort();
	edges.rewind();


	for (node_t u = 0; u < num_nodes; ++u) {
		for (; !edge_sorter.empty() && edge_sorter->first == u; ++edge_sorter) {
			out_stream << edge_sorter->second + 1 << " ";
		}
		for (; !edges.empty() && edges->first == u; ++edges) {
			out_stream << edges->second + 1 << " ";
		}
		out_stream << std::endl;
	}

	out_stream.close();
};

void export_as_edgelist(EdgeStream &edges, const std::string& filename) {
	edges.rewind();

	std::ofstream out_stream(filename, std::ios::trunc);

	using EdgeComparator = typename GenericComparator<edge_t>::Ascending;

	stxxl::sorter<edge_t, EdgeComparator> edge_sorter(EdgeComparator(), SORTER_MEM);
	for (; !edges.empty(); ++edges) {
		if (edges->second < edges->first)
			edge_sorter.push(edge_t(edges->second, edges->first));
		else
			edge_sorter.push(edge_t(edges->first, edges->second));
	}
	edge_sorter.sort();
	edges.rewind();

	for (; !edges.empty(); ++edges) {
		out_stream << edges->first << " " << edges->second << std::endl;
	}

	out_stream.close();
};

void export_as_snap(EdgeStream &edges, node_t num_nodes, const std::string& filename) {
	edges.rewind();
	edgeid_t num_edges = edges.size();

	std::ofstream out_stream(filename, std::ios::trunc);

	out_stream << "p " << num_nodes << " " << num_edges << " u u 0" << std::endl;

	using EdgeComparator = typename GenericComparator<edge_t>::Ascending;

	stxxl::sorter<edge_t, EdgeComparator> edge_sorter(EdgeComparator(), SORTER_MEM);
	for (; !edges.empty(); ++edges) {
		if (edges->second < edges->first)
			edge_sorter.push(edge_t(edges->second, edges->first));
		else
			edge_sorter.push(edge_t(edges->first, edges->second));
	}
	edge_sorter.sort();
	edges.rewind();

	for (; !edges.empty(); ++edges) {
		out_stream << edges->first << " " << edges->second << std::endl;
	}

	out_stream.close();
};
