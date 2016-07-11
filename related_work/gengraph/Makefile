CC = g++

CFLAGS=-Wall -g -O0 -fsanitize=address -fsanitize=signed-integer-overflow
CFLAGS=-Wall -g -O3

SRC = 
INCLUDE = include/
BIN = bin/

OBJ =$(BIN)random.o $(BIN)powerlaw.o $(BIN)degree_sequence.o $(BIN)box_list.o $(BIN)graph_molloy_hash.o $(BIN)graph_molloy_optimized.o

.cpp.o :
	$(CC) -c -o $(BIN)$@ $(INCLUDE)$<

all :
	make obj
	make graph
	make distrib

obj :
	$(CC) $(CFLAGS) -c -o $(BIN)graph_molloy_hash.o $(INCLUDE)graph_molloy_hash.cpp
	$(CC) $(CFLAGS) -c -o $(BIN)graph_molloy_optimized.o $(INCLUDE)graph_molloy_optimized.cpp
	$(CC) $(CFLAGS) -c -o $(BIN)degree_sequence.o $(INCLUDE)degree_sequence.cpp
	$(CC) $(CFLAGS) -c -o $(BIN)powerlaw.o $(INCLUDE)powerlaw.cpp
	$(CC) $(CFLAGS) -c -o $(BIN)random.o $(INCLUDE)random.cpp
	$(CC) $(CFLAGS) -c -o $(BIN)box_list.o $(INCLUDE)box_list.cpp

clean :
	rm -f $(BIN)* $(SRC)~* $(INCLUDE)~* $(SRC)*~ $(INCLUDE)*~

graph : $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) $(SRC)mr-connected.cpp -o $(BIN)graph

distrib : $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) $(SRC)distrib.cpp  -o $(BIN)distrib

