#include <iostream>
#include <defs.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <Utils/StreamPusher.h>
#include <Utils/export_metis.h>

int main() {
   const degree_t min_deg = 2;
   const degree_t max_deg = 100;
   const node_t num_nodes = 1000;


   HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree);
   MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2.0, num_nodes);

   StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
   hh_gen.generate();

   export_as_metis(hh_gen, "graph.metis");

   return 0;
}