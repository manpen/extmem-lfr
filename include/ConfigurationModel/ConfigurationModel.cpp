/**
 * Hung
 */
#include <defs.h>
#include "ConfigurationModel.h"

template class HavelHakimi_ConfigurationModel<HavelHakimiIMGenerator>;
template class HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator, TestNodeComparator>;
template class HavelHakimi_ConfigurationModel_Random<HavelHakimiIMGenerator, TestNodeRandomComparator>;