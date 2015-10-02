//
// Created by michael on 22.09.15.
//

#include "EdgeVector.h"

EdgeVector::EdgeVector(EdgeVector::vector_type &external_edges) : _external_edges(external_edges) {
}

void EdgeVector::flushEdges() {
    vector_type output_vector;
    output_vector.reserve(_external_edges.size());
    vector_type::bufwriter_type writer(output_vector);
    vector_type::bufreader_type reader(_external_edges);

    std::vector<edge_t> new_edges;
    new_edges.reserve(_internal_edges.size());
    for (auto e : _internal_edges) {
        if (e.second.first < e.second.second) {
            new_edges.push_back(e.second);
        } else {
            new_edges.emplace_back(e.second.second, e.second.first);
        }
    }

    std::sort(new_edges.begin(), new_edges.end());

    auto old_e = _internal_edges.begin();
    auto new_e = new_edges.begin();

    int_t read_id = 0;

    while (!reader.empty() || new_e != new_edges.end()) {
        // Skip elements that were already read
        while (old_e != _internal_edges.end() && old_e->first == read_id) {
            ++reader;
            ++read_id;
            ++old_e;
        }

        if (new_e != new_edges.end() && (reader.empty() || *new_e < *reader)) {
            writer << *new_e;
            ++new_e;
        }  else if (!reader.empty()) { // due to the previous while loop both could be empty now
            writer << *reader;
            ++reader;
        }
    }

    writer.finish();
    _external_edges.swap(output_vector);
    _internal_edges.clear();
}
