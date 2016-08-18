#include <iostream>
#include <limits>
#include <random>
#include <algorithm>
#include <string>
#include <Utils/IOStatistics.h>
#include <stxxl/sorter>
#include <omp.h>
#include <cmath>

template <unsigned int PayloadSize>
struct Data {
   uint64_t key;
   char payload[PayloadSize];

   Data() : key(0) {}
   Data(const uint64_t& k) : key(k) {}

   struct Compare {
      bool operator()(const Data& a, const Data& b) const {return a.key < b.key;}
      Data min_value() const {return {std::numeric_limits<uint64_t>::min()};};
      Data max_value() const {return {std::numeric_limits<uint64_t>::max()};};
   };
} __attribute__ ((packed));

struct Config {
   size_t minN;
   size_t maxN;
   unsigned int pointsPerDec;
   
   Config() : minN(10), maxN(10LLU * 1000 * 1000 * 1000), pointsPerDec(1) 
   {}
};


template <unsigned int PayloadSize, size_t BlockSize>
void benchmark(const Config& config, size_t mem) {
   using DataT = Data<PayloadSize>;
   using Sorter = stxxl::sorter<DataT, typename DataT::Compare, BlockSize>;

   const auto label = std::string("-pay") + std::to_string(PayloadSize) +
                      std::string("-mem") + std::to_string(std::lround(std::log2(mem))) + 
                      std::string("-blk") + std::to_string(std::lround(std::log2(BlockSize)));

   std::mt19937_64 random;

   // Test how fast we can generate data
   {
      const size_t size = 1 << 28;
      std::vector<DataT> data(size);

      auto time_before = omp_get_wtime();
      std::generate(data.begin(), data.end(), [&random] () {return DataT{random()};});
      auto time_after = omp_get_wtime();

      std::cout << "gen" << label << " Total time: " << (time_after - time_before) << "\n"
                   "gen" << label << " Through-put: " << (1.0 / (time_after - time_before) * size / 1000.) << "k"
      << std::endl;
   }

   // Almost unused sorter
   {
      unsigned int rounds = 0;
      double time_before = omp_get_wtime();
      double time_after = time_before;

      while( (time_after-time_before) < 10 ) {
         Sorter sorter(typename DataT::Compare{}, mem);
         for(unsigned int i=0; i < 7; i++)
            sorter.push(DataT{random()});
         sorter.sort();
         for(; !sorter.empty(); ++sorter);
         time_after = omp_get_wtime();
         rounds++;
      }

      std::cout << "init" << label << " Time per round " << (time_after-time_before)/rounds << "s" << std::endl;
   }

   unsigned int iteration = 0;
   for(size_t num=config.minN; num <= config.maxN; ++iteration, num = config.minN * std::pow(10.0, double(iteration)/config.pointsPerDec)) {
      Sorter sorter(typename DataT::Compare{}, mem);
      {
         IOStatistics stat("sort-push-" + std::to_string(num) + label);
         for(size_t i=num; --i; ) 
            sorter.push(DataT{random()});
         sorter.sort();
      }

      {
         IOStatistics stat("sort-read-" + std::to_string(num) + label);
         for(; !sorter.empty(); ++sorter);
      }
   }
}

template <unsigned int PayloadSize>
void benchmark_set(const Config& config, size_t mem) {
   benchmark<PayloadSize, 1 << 23>(config, mem);
   benchmark<PayloadSize, 1 << 21>(config, mem);
   benchmark<PayloadSize, 1 << 19>(config, mem);
   benchmark<PayloadSize, 1 << 16>(config, mem);
}

int main() { 
   Config config;
   
   for(size_t mem=1llu << 26; mem < (1llu << 32); mem = 2*mem) {
      benchmark_set<0>(config, mem);
      benchmark_set<1>(config, mem);
   }


}

