#include "ConfigurationModel.h"

void testStruct() {
	MultiNodeMsg mnm{30, 30};
	printf("MultiNodeMsg successfully created...\n");
}

void testSorter() {
	MultiNodeMsgSorter _multinode_sorter(MultiNodeMsgComparator{}, 64*1024*1024);
	printf("Sorter successfully created...\n");
}
