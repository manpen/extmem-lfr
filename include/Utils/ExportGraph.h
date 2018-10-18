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

void export_as_thrillbin(EdgeStream &edges, node_t num_nodes, const std::string& filename) {
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

template <typename EdgeStream>
void export_as_metis_sorted(EdgeStream &edges, const std::string& filename) {
	std::ofstream out_stream(filename, std::ios::trunc);

	using EdgeComparator = typename GenericComparator<edge_t>::Ascending;

	stxxl::sorter<edge_t, EdgeComparator> edge_sorter(EdgeComparator(), SORTER_MEM);
	node_t num_nodes = 0;
	for (; !edges.empty(); ++edges) {
		edge_sorter.push(edge_t(edges->first, edges->second));
		edge_sorter.push(edge_t(edges->second, edges->first));
		num_nodes = std::max(num_nodes, std::max(edges->first, edges->second));
	}
	num_nodes++;
	edge_sorter.sort();
	const edgeid_t num_edges = edge_sorter.size()/2;
	out_stream << num_nodes << " " << num_edges << " " << 0 << std::endl;

	for (node_t u = 0; u < num_nodes; ++u) {
		for (; !edge_sorter.empty() && edge_sorter->first == u; ++edge_sorter) {
			out_stream << edge_sorter->second + 1 << " ";
		}
		out_stream << std::endl;
	}

	std::cout << "[export_as_metis] Wrote " << num_edges << " edges with " << num_nodes << " nodes to file " << filename << std::endl;

	out_stream.close();
};

template <typename EdgeStream>
void export_as_metis_nonpointer(EdgeStream &edges, const std::string& filename, node_t num_nodes, bool isSorted = false) {
	std::ofstream out_stream(filename, std::ios::trunc);

	using EdgeComparator = typename GenericComparator<edge_t>::Ascending;

	edgeid_t num_edges = 0;

	if (!isSorted) {
		stxxl::sorter<edge_t, EdgeComparator> edge_sorter(EdgeComparator(), SORTER_MEM);
		node_t num_nodes = 0;
		for (; !edges.empty(); ++edges) {
			const auto edge = *edges;
			edge_sorter.push(edge_t(edge.first, edge.second));
			edge_sorter.push(edge_t(edge.second, edge.first));
		}
		edge_sorter.sort();

		edgeid_t num_edges = edge_sorter.size()/2;
		out_stream << num_nodes << " " << num_edges << " " << 0 << std::endl;

		for (node_t u = 0; u < num_nodes; ++u) {
			for (; !edge_sorter.empty() && edge_sorter->first == u; ++edge_sorter) {
				out_stream << edge_sorter->second + 1 << " ";
			}
			out_stream << std::endl;
		}
	} else {
		// sorted already
		num_edges = edges.size();
		out_stream << num_nodes << " " << num_edges << " " << 0 << std::endl;

		for (node_t u = 0; u < num_nodes; ++u) {
			for (; !edges.empty() && (*edges).first == u; ++edges) {
				out_stream << (*edges).second + 1 << " ";
			}
			out_stream << std::endl;
		}
	}
	std::cout << "[export_as_metis] Wrote " << num_edges << " edges with " << num_nodes << " nodes to file " << filename << std::endl;

	out_stream.close();
};

template <typename EdgeStream>
void export_as_metis_sorted_nonpointer_with_redirect(EdgeStream &edges, const std::string& filename, EdgeStream &out) {
	std::ofstream out_stream(filename, std::ios::trunc);

	using EdgeComparator = typename GenericComparator<edge_t>::Ascending;

	stxxl::sorter<edge_t, EdgeComparator> edge_sorter(EdgeComparator(), SORTER_MEM);
	node_t num_nodes = 0;
	for (; !edges.empty(); ++edges) {
		const auto edge = *edges;
		out.push(edge);
		edge_sorter.push(edge_t(edge.first, edge.second));
		edge_sorter.push(edge_t(edge.second, edge.first));
		num_nodes = std::max(num_nodes, std::max(edge.first, edge.second));
	}
	num_nodes++;
	edge_sorter.sort();
	const edgeid_t num_edges = edge_sorter.size()/2;
	out_stream << num_nodes << " " << num_edges << " " << 0 << std::endl;

	for (node_t u = 0; u < num_nodes; ++u) {
		for (; !edge_sorter.empty() && edge_sorter->first == u; ++edge_sorter) {
			out_stream << edge_sorter->second + 1 << " ";
		}
		out_stream << std::endl;
	}

	std::cout << "[export_as_metis] Wrote " << num_edges << " edges with " << num_nodes << " nodes to file " << filename << std::endl;

	out_stream.close();
};

template <typename EdgeStream>
void export_as_thrillbin_sorted(EdgeStream &edges, const std::string &filename, node_t num_nodes, stxxl::external_size_type max_bytes = (1ul<<30)) {
	size_t file_number = 0;

	auto next_filename = [&]() {
	    std::stringstream ss;
	    ss << filename << ".part-" << std::setw(5) << std::setfill('0') << file_number;
	    ++file_number;
	    return ss.str();
	};

	std::ofstream out_stream(next_filename(), std::ios::trunc | std::ios::binary);

	stxxl::vector<degree_t> half_degrees;

	edgeid_t num_edges = 0;
	for (node_t u = 0; u < num_nodes; ++u) {
		degree_t count = 0;
		for (; !edges.empty() && (*edges).first == u; ++edges) {
			count++;
			num_edges++;
		}
		half_degrees.push_back(count);
	}

	edges.rewind();

	stxxl::vector<degree_t>::iterator iter = half_degrees.begin();

	stxxl::external_size_type bytes_written = 0;

	for (node_t u = 0; u < num_nodes; ++u) {
		node_t deg = *iter;

		// assume the size of the neighbors needs 4 bytes
		if (bytes_written > 0 && bytes_written + deg * 4 + 4 > max_bytes) {
		    out_stream.close();
		    // This does not compile with GCC < 5 because of a missing move assignment operator!
		    // see https://gcc.gnu.org/bugzilla/show_bug.cgi?id=54316 for a related issue
		    out_stream = std::ofstream(next_filename(), std::ios::trunc | std::ios::binary);
		    bytes_written = 0;
		}

		if (deg < 128) {
			out_stream.write(reinterpret_cast<const char*>(&deg), 1);
			++bytes_written;
		} else {
			while (deg > 0) {
				uint8_t tmp = (deg & 0x7f);
				tmp |= 0x80;
				out_stream.write(reinterpret_cast<const char*>(&tmp), sizeof tmp);
				++bytes_written;
				deg = deg >> 7;
			}
			const uint8_t zero = 0;
			out_stream.write(reinterpret_cast<const char*>(&zero), sizeof zero);
			++bytes_written;
		}

		bytes_written += 4llu * deg;
		for (; !edges.empty() && (*edges).first == u; ++edges) {
			out_stream.write(reinterpret_cast<const char*>(&((*edges).second)), 4);
		}
		iter++;
	}

	edges.rewind();

	std::cout << "[export_as_thrillbinary] Wrote " << num_edges << " edges with " << num_nodes << " nodes to file " << filename << std::endl;

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
