#include <iostream>
#include <queue>
#include <vector>
#include <tuple>

#include <defs.h>
#include <Utils/ThrillBinaryReader.h>

#include <list>
#include <algorithm>

#include <glob.h>
#include <list>
#include <string>

constexpr size_t PHASES_PER_STEP = 2;

std::list<std::string> glob(const std::string& pat){
    glob_t glob_result;
    glob(pat.c_str(),GLOB_TILDE,NULL,&glob_result);
    std::list<std::string> ret;

    for(unsigned int i=0;i<glob_result.gl_pathc;++i){
        ret.push_back(std::string(glob_result.gl_pathv[i]));
    }
    globfree(&glob_result);

    return ret;
}

struct AnnotatedReader : public ThrillBinaryReader {
    uint32_t graph;
    uint32_t step;

    AnnotatedReader() : ThrillBinaryReader() {}
};

struct AnnotatedFile {
    uint32_t step;
    uint32_t graph;
    std::string file;

    AnnotatedFile(uint32_t step = 0, uint32_t graph = 0, const std::string& file = "")
            : step(step), graph(graph), file(file)
    {}

    bool operator< (const AnnotatedFile& o) const {
        return std::tie(step, graph) < std::tie(o.step, o.graph);
    }
};

std::vector<AnnotatedFile> annotate_filenames(const std::list<std::string> files) {
    std::vector<AnnotatedFile> result;
    result.reserve(files.size());

    for(const auto& file : files) {
        if (file.substr(file.size() - 3, 3) != ".tb")
            continue;

        const auto sep2 = file.find_last_of('-');
        const auto sep1 = file.find_last_of('-', sep2 - 1);

        if (sep1 == std::string::npos)
            continue;

        const uint32_t phase = atoi(file.substr(sep2+1, file.size() - sep2 - 4).c_str());
        const uint32_t graph = atoi(file.substr(sep1+1, sep2 - sep1 - 1).c_str());

        if (phase % PHASES_PER_STEP)
            continue;

        result.emplace_back(phase / PHASES_PER_STEP, graph, file);
    }

    std::sort(result.begin(), result.end());

    return result;
}

struct PQItem {
    edge_t edge;
    uint32_t queue;

    PQItem(const edge_t edge = edge_t{0,0}, const size_t queue = 0)
        : edge(edge), queue(queue)
    {}

    bool operator<(const PQItem& other) const {
        return std::tie(edge, queue) > std::tie(other.edge, other.queue);
    }
};

struct AnnotatedEdge {
    uint32_t step;
    uint32_t graph;
    edge_t edge;

    AnnotatedEdge(uint32_t step = 0, uint32_t graph = 0, edge_t edge = {0,0})
        : step(step), graph(graph), edge(edge)
    {}
};


int main(int argc, char* argv[]) {
    std::vector<AnnotatedFile> annotated_files;

    // glob all files
    {
        std::list<std::string> files;

        for(int i=1; i < argc; ++i) {
            const auto globbed = glob(argv[i]);
            files.insert(files.end(), globbed.cbegin(), globbed.cend());
        }

        std::cout << "Found " << files.size() << " files." << std::endl;
        if (files.empty())
            abort();


        annotated_files = annotate_filenames(files);
        std::cout << "Annotated " << annotated_files.size() << " files." << std::endl;
        if (files.empty())
            abort();

        if (annotated_files.empty())
            abort();
    }


    std::vector<AnnotatedReader> readers(annotated_files.size());
    std::vector<node_t> counts(argc, 0);

    size_t reader_active = readers.size();
    std::priority_queue<PQItem> buffer;

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

    auto fetch_and_reload = [&] () {
        PQItem result = buffer.top();
        buffer.pop();
        insert_to_buffer(result.queue);

        return AnnotatedEdge{readers[result.queue].step, readers[result.queue].graph, result.edge};
    };

    // open file and fill buffer
    for(size_t i=0; i < readers.size(); i++) {
        auto& reader = readers[i];
        const auto& file = annotated_files[i];

        reader.open(file.file);
        reader.step = file.step;
        reader.graph = file.graph;

        if (reader.empty()) {
            std::cout << "File " << argv[i] << " seems empty" << std::endl;
        }

        insert_to_buffer(i);
    }

    // consume readers
    if (!buffer.empty()) {
        auto last_edge = fetch_and_reload();
        uint_t multiplicity = 1;
        while(!buffer.empty()) {
            const auto current_edge = fetch_and_reload();
            if (current_edge.edge != last_edge.edge) {
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