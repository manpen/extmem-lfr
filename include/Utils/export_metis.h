#pragma once

#include <EdgeStream.h>
#include <fstream>
#include <stxxl/sorter>
#include <defs.h>
#include <GenericComparator.h>

template <typename EdgeStream>
void export_as_metis(EdgeStream &edges, const std::string& filename) {
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
void export_as_metis_nonpointer(EdgeStream &edges, const std::string& filename) {
	std::ofstream out_stream(filename, std::ios::trunc);

	using EdgeComparator = typename GenericComparator<edge_t>::Ascending;

	stxxl::sorter<edge_t, EdgeComparator> edge_sorter(EdgeComparator(), SORTER_MEM);
	node_t num_nodes = 0;
	for (; !edges.empty(); ++edges) {
        auto edge = *edges;
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