//
// Created by michael on 09.07.15.
//

#include "HavelHakimiGenerator.h"

HavelHakimiGenerator &HavelHakimiGenerator::operator++() {
    if (prioQueue.empty() && current_node_degree.degree > 0) {
        STXXL_ERRMSG("Degree sequence not realizable, node " << current_node_degree.node << " should have got " << current_node_degree.degree << " more neighbors");
    }

    if (current_node_degree.degree <= 0 || (prioQueue.empty() && !stack.empty())) {

        while (!stack.empty()) {
            prioQueue.push(stack.top());
            stack.pop();
        }

        current_node_degree = prioQueue.top();
        prioQueue.pop();
    }

    if (current_node_degree.degree > 0 && !prioQueue.empty()) {
        node_degree partner = prioQueue.top();
        prioQueue.pop();
        current = {current_node_degree.node, partner.node};
        --partner.degree;
        --current_node_degree.degree;
        if (partner.degree > 0) {
            stack.push(partner);
        }
    }

    return *this;
}
