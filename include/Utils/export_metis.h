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

template <typename Edge64Stream>
void export_as_metis_64(Edge64Stream &edges, const std::string& filename) {
	std::ofstream out_stream(filename, std::ios::trunc);

   using Edge64Comparator = typename GenericComparator<edge64_t>::Ascending;

	stxxl::sorter<edge64_t, Edge64Comparator> edge64_sorter(Edge64Comparator(), SORTER_MEM);
   multinode_t num_nodes = 0;
	for (; !edges.empty(); ++edges) {
      edge64_sorter.push(edge64_t((*edges).first, (*edges).second));
      edge64_sorter.push(edge64_t((*edges).second, (*edges).first));
      num_nodes = std::max(num_nodes, std::max((*edges).first, (*edges).second));
	}
   num_nodes++;
	edge64_sorter.sort();
   const edge64id_t num_edges = edge64_sorter.size()/2;
   out_stream << num_nodes << " " << num_edges << " " << 0 << std::endl;

	for (multinode_t u = 0; u < num_nodes; ++u) {
		for (; !edge64_sorter.empty() && edge64_sorter->first == u; ++edge64_sorter) {
			out_stream << edge64_sorter->second + 1 << " ";
		}
		out_stream << std::endl;
	}

   std::cout << "[export_as_metis] Wrote " << num_edges << " edges with " << num_nodes << " nodes to file " << filename << std::endl;

	out_stream.close();
};
