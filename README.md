# External Memory Graph Generators

Project for implementing external memory graph generators especially for clustering.

Goal: Implement an external memory version of the LFR generator.

This project uses the STXXL library via CMake project files.

## Moving to a new machine
### GTest on debian
```
sudo apt-get install libgtest-dev
cd /usr/src/gtest/
sudo mkdir build
cd build
sudo cmake ..
sudo make
sudo cp *.a /usr/lib
```

### Doxygen
Doxygen version >= 1.8.2 is required for C++11 features like using-style typedef

### Building binaries
After running
```
git clone https://github.com/massive-graphs/extmem-lfr.git
cd extmem-lfr
```
initialise submodules with
```
git submodule init
git submodule update --recursive
```
and get a debug build with all binaries by
```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j
```
or a release build with
```
mkdir release
cd release
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

### Curveball Implementations
Benchmarks can be done for powerlaw graph instances.
The benchmark file uses the following parameters
```
-n	Number of nodes
-a	Minimum degree
-b	Maximum degree
-g	Degree exponent (Use -2 for gamma = 2)

-r	Number of global trades
-c	Number of macrochunks in a global trade
-z	Number of batches in a macrochunk
-t	Number of threads while trading
-y	Size of insertion buffer for each thread

-i	Block size used by sorters in Byte (default: 2GiB) 
```
An exemplary run would be (leaving out block size as default)
```
./curveball_benchmark -n 10000 -a 10 -b 200 -g -2 -r 10 -c 4 -z 8 -t 8 -y 16
```
