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