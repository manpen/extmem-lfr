#include "RandomSeed.h"

// this introduces a memory leak; but i can live with that
RandomSeed* RandomSeed::_instance = new RandomSeed();