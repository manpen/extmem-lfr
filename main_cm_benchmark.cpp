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

 	std::ofstream cmhh_file("cmhh_benchmark.dat");

	for (degree_t max_deg = 10000000; max_deg <= pow(10, runs); max_deg*= 10) {

		const multinode_t num_nodes = static_cast<multinode_t>(max_deg)*10;

		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
		MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

		StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
		hh_gen.generate();

		HavelHakimi_ConfigurationModel<HavelHakimiIMGenerator> cmhh(hh_gen, 223224, num_nodes);

		// Start benchmark here
		// I/O-measurements
		//stxxl::stats* Stats = stxxl::stats::get_instance();
		//stxxl::stats_data stats_begin(*Stats);
		clock_t startTime = clock();

		// Run Code here
		cmhh.run();

		// End benchmark
		std::stringstream outLine;
        	//outLine << max_deg << "\t" << stats_begin.get_elapsed_time() << " #CMCRCHH\n";
        	outLine << max_deg << "\t" << double ( clock() - startTime ) / (double) CLOCKS_PER_SEC << " #CRCHH\n";
		std::string outLineStr = outLine.str();
		cmhh_file << outLineStr;
		outLine.str(std::string());

		cmhh.clear();

	}

	cmhh_file.close();


	std::cout << "Benchmark for ConfigurationModel(Random)..." << std::endl;

	std::ofstream cmr_file("cmr_benchmark.dat");

	// ConfigurationModel Random

	for (degree_t max_deg = 10000000; max_deg <= pow(10, runs); max_deg*= 10) {

		const multinode_t num_nodes = static_cast<multinode_t>(max_deg)*10;

		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
		MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

		StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
		hh_gen.generate();

		HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator> cmhh(hh_gen, num_nodes);

		// Start benchmark here
		// I/O-measurements
		//stxxl::stats* Stats = stxxl::stats::get_instance();
		//stxxl::stats_data stats_begin(*Stats);
		clock_t startTime = clock();

		// Run Code here
		cmhh.run();

		// End benchmark
		std::stringstream outLine;
        	//outLine << max_deg << "\t" << stats_begin.get_elapsed_time() << " #CMCRCR\n";
        	outLine << max_deg << "\t" << double( clock() - startTime ) / (double) CLOCKS_PER_SEC << " R\n";
		std::string outLineStr = outLine.str();
        	cmr_file << outLineStr;
        	outLine.str(std::string());

		cmhh.clear();
		}	

	cmr_file.close();

	return 0;
}
