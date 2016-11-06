#include <defs.h>

#include <ConfigurationModel.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <Utils/StreamPusher.h>
#include <time.h>

//benchmarks
#include <stxxl/stats>

int main(int argc, char* argv[]) {
	const degree_t min_deg = 5;

	int runs = std::stoi(argv[1]);

	// ConfigurationModel HavelHakimi

	std::cout << "Benchmark for ConfigurationModel(HavelHakimi)..." << std::endl;

 	//std::ofstream cmhh_file("cmhh_benchmark.dat");

	for (degree_t max_deg = 10; max_deg <= pow(10, runs); max_deg*= 10) {

		const multinode_t num_nodes = static_cast<multinode_t>(max_deg)*10;

		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
		MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

		StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
		hh_gen.generate();

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

		cmhh.clear();

        cmhh_file.close();
	}

	std::cout << "Benchmark for ConfigurationModel(Random)..." << std::endl;

	std::ofstream cmr_file("cmr_benchmark.dat");

	// ConfigurationModel Random

	for (degree_t max_deg = 10; max_deg <= pow(10, runs); max_deg*= 10) {

		const multinode_t num_nodes = static_cast<multinode_t>(max_deg)*10;

		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
		MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

		StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
		hh_gen.generate();

		HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator> cmhh(hh_gen, num_nodes);

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

		cmhh.clear();

		cmr_file.close();
		}	

	cmr_file.close();

	return 0;
}
