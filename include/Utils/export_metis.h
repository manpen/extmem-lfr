#pragma once

#include <EdgeStream.h>
#include <fstream>
#include <stxxl/sorter>
#include <defs.h>
#include <GenericComparator.h>

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
