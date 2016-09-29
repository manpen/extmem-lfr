#include <ConfigurationModel.h>

int main() {

    constexpr int runs = 20;
    const std::vector<uint32_t> dmin = { 1, 2, 5, 10};//{ 20 };//{ 1, 2, 5, 10}; //, 20 };
    const std::vector<uint32_t> dmax = { 20000 };//{ 1000, 2000, 5000, 10000 };//{ 20000 };//{ 1000, 2000, 5000, 10000}; //, 20000 };
    
    // random seed?
    //stxxl::random_number32 rand32;
    stxxl::srandom_number32(time(NULL));

    // #Nodes is dmax times 10.
    // We do 20 Runs, for each pair of dmin and dmax
    for (auto const& deg_min : dmin) {
        for (auto const& deg_max : dmax) {
            std::cout << "Processing for dmin: " << deg_min << " and dmax: " << deg_max << std::endl;
            
            
            // Configurationmodel CRC
            {
                // format string filename
                std::stringstream fmt;
                fmt << "CM_" << deg_min << "_" << deg_max << ".dat";
                std::string filename = fmt.str();
               
                // open file
                std::ofstream outFile(filename);

                const uint32_t num_nodes = deg_max * 10;
                
                for (int i = 0; i < runs; ++i) {
                    auto degrees = MonotonicPowerlawRandomStream<false>(deg_min, deg_max, -2, num_nodes);
                    ConfigurationModel<> cm(degrees, 179273927, num_nodes);
                    cm.run();

                    // count self-loops and multi-edges
                    long loops = 0;
                    long multi = 0;
                    long times = 0;
                    
                    bool prev_multi = false;

                    auto prev_edge = *cm;
                    if (prev_edge.is_loop())
                        ++loops;
                    
                    ++cm;

                    for (; !cm.empty(); ++cm) {
                        auto & edge = *cm;
                        
                        // self-loop found
                        if (edge.is_loop()) {
                            ++loops;
                            if (prev_multi)
                                ++times;
                            prev_edge = edge;
                            prev_multi = false;
                            continue;
                        }

                        // multi-edge found
                        if (prev_edge == edge) {
                            ++times;
                            if (!prev_multi) {
                                ++multi;
                                prev_multi = true;
                            }
                            prev_edge = edge;
                            continue;
                        }

                        if (prev_multi)
                            ++times;
                        prev_edge = edge;

                        prev_multi = false;
                    }

                    if (prev_multi)
                        ++times;
                    
                    cm.clear();

                    std::stringstream outLine;
                    outLine << loops << "\t" << multi << "\t" << times << " #CRCCM\n";

                    std::string outLineStr = outLine.str();
                    outFile << outLineStr;

                    outLine.str(std::string());
                }

                outFile.close();
            }

            // Configurationmodel Random
            {
                // format string filename
                std::stringstream fmt;
                fmt << "CM_r_" << deg_min << "_" << deg_max << ".dat";
                std::string filename = fmt.str();
               
                // open file
                std::ofstream outFile(filename);

                const uint32_t num_nodes = deg_max * 10;
                
                for (int i = 0; i < runs; ++i) {
                    auto degrees = MonotonicPowerlawRandomStream<false>(deg_min, deg_max, -2, num_nodes);
                    ConfigurationModelRandom<> cmr(degrees, 179273927, num_nodes);
                    cmr.run();

                    // count self-loops and multi-edges
                    long loops = 0;
                    long multi = 0;
                    long times = 0;
                    
                    bool prev_multi = false;

                    auto prev_edge = *cmr;
                    if (prev_edge.is_loop())
                        ++loops;
                    
                    ++cmr;

                    for (; !cmr.empty(); ++cmr) {
                        auto & edge = *cmr;
                        
                        // self-loop found
                        if (edge.is_loop()) {
                            ++loops;
                            if (prev_multi)
                                ++times;
                            prev_edge = edge;
                            prev_multi = false;
                            continue;
                        }

                        // multi-edge found
                        if (prev_edge == edge) {
                            ++times;
                            if (!prev_multi) {
                                ++multi;
                                prev_multi = true;
                            }
                            prev_edge = edge;
                            continue;
                        }

                        if (prev_multi)
                            ++times;
                        prev_edge = edge;

                        prev_multi = false;
                    }

                    if (prev_multi)
                        ++times;
                    
                    cmr.clear();

                    std::stringstream outLine;
                    outLine << loops << "\t" << multi << "\t" << times << " #R64CM\n";

                    std::string outLineStr = outLine.str();
                    outFile << outLineStr;

                    outLine.str(std::string());
                }

                outFile.close();
            }
        }
    }

    return 0;
}
