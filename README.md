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