#include <iostream>
#include <queue>
#include <vector>
#include <tuple>

#include <defs.h>
#include <Utils/ThrillBinaryReader.h>


struct pq_item {
    edge_t edge;
    size_t queue;

    pq_item(const edge_t edge = edge_t{0,0}, const size_t queue = 0)
        : edge(edge), queue(queue)
    {}

    bool operator<(const pq_item& other) const {
        return std::tie(edge, queue) > std::tie(other.edge, other.queue);
    }
};

int main(int argc, char* argv[]) {
    std::vector<ThrillBinaryReader> readers(argc - 1);
    std::vector<node_t> counts(argc, 0);

    size_t reader_active = readers.size();
    std::priority_queue<pq_item> buffer;

    auto insert_to_buffer = [&] (size_t idx) {
        auto& reader = readers.at(idx);

        if (reader.empty()) {
            reader_active--;
            std::cout << "Consumed reader " << idx << " after " << reader.edges_read() << " edges\n";
            return;
        }

        buffer.emplace(*reader, idx);
        ++reader;
    };

    auto fetch_and_reload = [&] () -> edge_t {
        pq_item result = buffer.top();
        buffer.pop();
        insert_to_buffer(result.queue);
        return result.edge;
    };

    // open file and fill buffer
    for(size_t i=1; i< static_cast<size_t>(argc); i++) {
        auto& reader = readers.at(i - 1);

        reader.open(argv[i]);

        if (reader.empty()) {
            std::cout << "File " << argv[i] << " seems empty" << std::endl;
        }

        insert_to_buffer(i - 1);
    }

    // consume readers
    if (!buffer.empty()) {
        edge_t last_edge = fetch_and_reload();
        uint_t multiplicity = 1;
        while(!buffer.empty()) {
            const edge_t current_edge = fetch_and_reload();
            if (current_edge != last_edge) {
                counts.at(multiplicity)++;
                multiplicity = 0;
                last_edge = current_edge;
            }

            ++multiplicity;
        }
        counts.at(multiplicity)++;
    }

    // output result
    edgeid_t edges = 0;
    edgeid_t unique_edges = 0;
    for(size_t i=0; i<counts.size(); ++i) {
        std::cout << i << "\t" << counts[i] << " # DISTR\n";
        edges += i * counts[i];
        unique_edges += counts[i];
    }

    std::cout << "Edges found: " << edges << "\n";
    std::cout << "Unique edges: " << unique_edges << " ("
              << (1.0 * unique_edges / readers.size() / readers[0].edges_read()) << ")\n";

    // Check every reader contributed the same number of edges;
    {
        auto dump_edges_per_reader = [&] () {
            for(unsigned int i=0; i < readers.size(); ++i)
                std::cout << " " << i << "\tEdges: " << readers[i].edges_read() << "\t File: " << argv[i+1];
        };

        for(unsigned int i=1; i < readers.size(); ++i) {
            if (readers[i].edges_read() != readers[0].edges_read()) {
                dump_edges_per_reader();
                return -1;
            }
        }

        const edgeid_t expected = readers.size() * readers[0].edges_read();
        if (edges != expected) {
            std::cout << "Edge mismatch -- expected " << expected << " counts\n";
            return -1;
        }
    }

    std::cout << "Done.\n";

    return 0;
}