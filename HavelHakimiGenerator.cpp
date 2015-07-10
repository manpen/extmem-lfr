//
// Created by michael on 09.07.15.
//

#include "HavelHakimiGenerator.h"

HavelHakimiGenerator &HavelHakimiGenerator::operator++() {
    if (prioQueue.empty() && current_node_degree.degree > 0) {
        STXXL_ERRMSG("Degree sequence not realizable, node " << current_node_degree.node << " should have got " << current_node_degree.degree << " more neighbors");
    }

    // if no more edges need to be generated anymore for the current node or cannot be generated anymore (PQ empty)
    if (current_node_degree.degree <= 0 || (prioQueue.empty() && !stack.empty())) {
        // remove nodes from stack and put them back into the PQ
        while (!stack.empty()) {
            prioQueue.push(stack.top());
            stack.pop();
        }

        // new source node
        if (!prioQueue.empty()) {
            current_node_degree = prioQueue.top();
            prioQueue.pop();
        }
    }

    // if the current node needs edges and we can find targets for edges in the PQ
    if (current_node_degree.degree > 0 && !prioQueue.empty()) {
        // target node is node with highest degree from PQ
        node_degree partner = prioQueue.top();
        prioQueue.pop();
        // set current edge
        current = {current_node_degree.node, partner.node};
        // decrease degrees of both nodes
        --partner.degree;
        --current_node_degree.degree;
        // if the target node needs to get more edges add it to the stack so it gets re-added to the PQ
        if (partner.degree > 0) {
            stack.push(partner);
        }
    } else {
    // as in the first part we already tried to make edge generation possible not being able to create edges
    // here means that we cannot generated any edges anymore
        if (current_node_degree.degree > 0) {
            STXXL_ERRMSG("Degree sequence not realizable, node " << current_node_degree.node << " should have got " << current_node_degree.degree << " more neighbors");
        }

        is_empty = true;
    }

    return *this;
}
