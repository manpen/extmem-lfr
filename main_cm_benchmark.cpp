#include <defs.h>

#include <ConfigurationModel.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <Utils/StreamPusher.h>
#include <time.h>

//benchmarks
#include <stxxl/stats>

int main(int argc, char* argv[]) {
	if (argc <= 0)
        return 0;

	const int runs = std::stoi(argv[1]);
    const degree_t min_deg = std::stoi(argv[2]);
    const double ratio = std::stoi(argv[3]);

	// ConfigurationModel HavelHakimi

	std::cout << "Benchmark for ConfigurationModel(HavelHakimi)..." << std::endl;

	for (multinode_t num_nodes = 100; num_nodes <= pow(10, runs + 1); num_nodes *= 10) {

		const degree_t max_deg = static_cast<degree_t>(num_nodes / ratio);
        if (max_deg > num_nodes)
            continue;
        if (max_deg < min_deg)
            continue;

        const degree_t threshold = max_deg / 5;

		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, threshold);
		MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

		StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
		hh_gen.generate();

        std::cout << "Max degree fed to HH: " << hh_gen.maxDegree() << "; "
                     "Nodes with degree above  " << threshold << ": " << hh_gen.nodesAboveThreshold()
                  << std::endl;

		HavelHakimi_ConfigurationModel<HavelHakimiIMGenerator> cmhh(hh_gen, 223224, num_nodes);

		// Start benchmark here
		// Stats-measurements
		stxxl::stats* Stats = stxxl::stats::get_instance();
		stxxl::stats_data stats_begin(*Stats);

		// Run Code here
		cmhh.run();

		std::stringstream fmt;
        fmt << "cm_crc" << max_deg << ".log";
        std::string fileName = fmt.str();
        std::ofstream cmhh_file(fileName);

		// End benchmark
		cmhh_file << "min_deg set to: " << min_deg << std::endl;
		cmhh_file << "max_deg set to: " << max_deg << std::endl;
        cmhh_file << "edges set to: " << cmhh.size() << std::endl;
        cmhh_file << "nodes set to: " << num_nodes << std::endl;
		cmhh_file << (stxxl::stats_data(*Stats) - stats_begin);
		//outLine.str(std::string());

		// count self-loops and multi-edges
        long loops = 0;
        long multi = 0;
        long times = 0;
        
        bool prev_multi = false;

        auto prev_edge = *cmhh;
        if (prev_edge.is_loop())
            ++loops;
        
        ++cmhh;

        for (; !cmhh.empty(); ++cmhh) {
            auto & edge = *cmhh;
            
            // self-loop found
            if (edge.is_loop()) {
                ++loops;
                if (prev_multi)
                    ++times;
                prev_edge = edge;
                prev_multi = false;
                continue;
            }

            // multi-edge found
            if (prev_edge == edge) {
                ++times;
                if (!prev_multi) {
                    ++multi;
                    prev_multi = true;
                }
                prev_edge = edge;
                continue;
            }

            if (prev_multi)
                ++times;
            prev_edge = edge;

            prev_multi = false;
        }

        if (prev_multi)
            ++times;

        cmhh_file << "self_loops: " << loops << std::endl;
		cmhh_file << "multi_edges: " << multi << std::endl;
        cmhh_file << "multi_edges quantities: " << times << std::endl;
		
        cmhh.clear();

        cmhh_file.close();
	}

	std::cout << "Benchmark for ConfigurationModel(Random)..." << std::endl;

	// ConfigurationModel Random

	for (multinode_t num_nodes = 100; num_nodes <= pow(10, runs + 1); num_nodes*= 10) {

		const degree_t max_deg = static_cast<degree_t>(num_nodes / ratio);

		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
		MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

		StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
		hh_gen.generate();

		HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator, TestNodeComparator> cmhh(hh_gen, num_nodes);

		// Start benchmark here
		// Stats-measurements
		stxxl::stats* Stats = stxxl::stats::get_instance();
		stxxl::stats_data stats_begin(*Stats);

		// Run Code here
		cmhh.run();

		std::stringstream fmt;
        fmt << "cm_r" << max_deg << ".log"; 
        std::string fileName = fmt.str();
        std::ofstream cmr_file(fileName);

		// End benchmark
		cmr_file << "min_deg set to: " << min_deg << std::endl;
		cmr_file << "max_deg set to: " << max_deg << std::endl;
        cmr_file << "edges set to: " << cmhh.size() << std::endl;
        cmr_file << "nodes set to: " << num_nodes << std::endl;
		cmr_file << (stxxl::stats_data(*Stats) - stats_begin);

		// count self-loops and multi-edges
        long loops = 0;
        long multi = 0;
        long times = 0;
        
        bool prev_multi = false;

        auto prev_edge = *cmhh;
        if (prev_edge.is_loop())
            ++loops;
        
        ++cmhh;

        for (; !cmhh.empty(); ++cmhh) {
            auto & edge = *cmhh;
            
            // self-loop found
            if (edge.is_loop()) {
                ++loops;
                if (prev_multi)
                    ++times;
                prev_edge = edge;
                prev_multi = false;
                continue;
            }

            // multi-edge found
            if (prev_edge == edge) {
                ++times;
                if (!prev_multi) {
                    ++multi;
                    prev_multi = true;
                }
                prev_edge = edge;
                continue;
            }

            if (prev_multi)
                ++times;
            prev_edge = edge;

            prev_multi = false;
        }

        if (prev_multi)
            ++times;

        cmr_file << "self_loops: " << loops << std::endl;
		cmr_file << "multi_edges: " << multi << std::endl;
        cmr_file << "multi_edges quantities: " << times << std::endl;

		cmhh.clear();

		cmr_file.close();

		}	

// ConfigurationModel RandomRandom

    for (degree_t max_deg = 10; max_deg <= pow(10, runs); max_deg*= 10) {

        const multinode_t num_nodes = static_cast<multinode_t>(max_deg)*10;

        HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
        MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

        StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
        hh_gen.generate();

        HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator, TestNodeRandomComparator> cmhh(hh_gen, num_nodes);

        // Start benchmark here
        // Stats-measurements
        stxxl::stats* Stats = stxxl::stats::get_instance();
        stxxl::stats_data stats_begin(*Stats);

        // Run Code here
        cmhh.run();

        std::stringstream fmt;
        fmt << "cm_tupr" << max_deg << ".log"; 
        std::string fileName = fmt.str();
        std::ofstream cmr_file(fileName);

        // End benchmark
        cmr_file << "min_deg set to: " << min_deg << std::endl;
        cmr_file << "max_deg set to: " << max_deg << std::endl;
        cmr_file << "edges set to: " << cmhh.size() << std::endl;
        cmr_file << "nodes set to: " << num_nodes << std::endl;
        cmr_file << (stxxl::stats_data(*Stats) - stats_begin);

        // count self-loops and multi-edges
        long loops = 0;
        long multi = 0;
        long times = 0;
        
        bool prev_multi = false;

        auto prev_edge = *cmhh;
        if (prev_edge.is_loop())
            ++loops;
        
        ++cmhh;

        for (; !cmhh.empty(); ++cmhh) {
            auto & edge = *cmhh;
            
            // self-loop found
            if (edge.is_loop()) {
                ++loops;
                if (prev_multi)
                    ++times;
                prev_edge = edge;
                prev_multi = false;
                continue;
            }

            // multi-edge found
            if (prev_edge == edge) {
                ++times;
                if (!prev_multi) {
                    ++multi;
                    prev_multi = true;
                }
                prev_edge = edge;
                continue;
            }

            if (prev_multi)
                ++times;
            prev_edge = edge;

            prev_multi = false;
        }

        if (prev_multi)
            ++times;

        cmr_file << "self_loops: " << loops << std::endl;
        cmr_file << "multi_edges: " << multi << std::endl;
        cmr_file << "multi_edges quantities: " << times << std::endl;

        cmhh.clear();

        cmr_file.close();

        }   

	return 0;
}
