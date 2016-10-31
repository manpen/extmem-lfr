#include <iostream>
#include <defs.h>
#include <ConfigurationModel.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <Utils/StreamPusher.h>
#include <Utils/export_metis.h>

int main() {
   const degree_t min_deg = 2;
   const degree_t max_deg = 1000;
   const node_t num_nodes = 10000;


   HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
   MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

   StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
   hh_gen.generate();

   HavelHakimi_ConfigurationModel<HavelHakimiIMGenerator> cmhh(hh_gen, 223224, num_nodes);
   cmhh.run();

   export_as_metis_64(cmhh, "cm_hh.metis");

   return 0;
}