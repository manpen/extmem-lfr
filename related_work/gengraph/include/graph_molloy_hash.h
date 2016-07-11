#ifndef GRAPH_MOLLOY_HASH_H
#define GRAPH_MOLLOY_HASH_H

#include "definitions.h"
#include "hash.h"
#include "degree_sequence.h"

#include <string.h>
#include <assert.h>
// This class handles graphs with a constant degree sequence.

#define FINAL_HEURISTICS        0
#define GKAN_HEURISTICS         1
#define FAB_HEURISTICS          2
#define OPTIMAL_HEURISTICS      3
#define BRUTE_FORCE_HEURISTICS  4
#define DISCONNECTED 		5

//****************************
//  class graph_molloy_hash
//****************************

class graph_molloy_hash {

private:
  // Number of vertices
  node_t n;
  //Number of arcs ( = #edges * 2 )
  edgeid_t a;
  //Total size of links[]
  edgeid_t size;
  // The degree sequence of the graph
  degree_t *deg;
  // The array containing all links
  node_t *links;
  // The array containing pointers to adjacency list of every vertices
  node_t **neigh;
  // Counts total size
  void compute_size();
  // Build neigh with deg and links
  void compute_neigh();
  // Allocate memory according to degree_sequence (for constructor use only!!)
  edgeid_t alloc(degree_sequence &);
  // Add edge (a,b). Return FALSE if vertex a is already full.
  // WARNING : only to be used by havelhakimi(), restore() or constructors
  inline bool add_edge(node_t a,node_t b,degree_t *realdeg) {
    degree_t deg_a = realdeg[a];
    if(deg_a == deg[a]) return false;
    // Check that edge was not already inserted
    assert(fast_search(neigh[a],node_t ((a==n-1 ? links+size : neigh[a+1])-neigh[a]),b)==NULL);
    assert(fast_search(neigh[b],node_t ((b==n-1 ? links+size : neigh[b+1])-neigh[b]),a)==NULL);
    assert(deg[a]<deg_a);
    degree_t deg_b = realdeg[b];
    if(IS_HASH(deg_a)) *H_add(neigh[a],HASH_EXPAND(deg_a),b)=b;
    else neigh[a][deg[a]] = b;
    if(IS_HASH(deg_b)) *H_add(neigh[b],HASH_EXPAND(deg_b),a)=a;
    else neigh[b][deg[b]] = a;
    deg[a]++;
    deg[b]++;
    // Check that edge was actually inserted
    assert(fast_search(neigh[a],node_t ((a==n-1 ? links+size : neigh[a+1])-neigh[a]),b)!=NULL);
    assert(fast_search(neigh[b],node_t ((b==n-1 ? links+size : neigh[b+1])-neigh[b]),a)!=NULL);
    return true;
  }
  // Swap edges
  inline void swap_edges(node_t from1, node_t to1, node_t from2, node_t to2) {
    H_rpl(neigh[from1],deg[from1],to1,to2);
    H_rpl(neigh[from2],deg[from2],to2,to1);
    H_rpl(neigh[to1],deg[to1],from1,from2);
    H_rpl(neigh[to2],deg[to2],from2,from1);
  }
  // Backup graph [sizeof(int) bytes per edge]
  node_t * backup();
  // Test if vertex is in an isolated component of size<K
  bool isolated(node_t v, node_t K, node_t *Kbuff, bool *visited);
  // Pick random edge, and gives a corresponding vertex
  inline node_t pick_random_vertex() { 
    node_t v;
    do v = links[my_random()%size]; while(v==HASH_NONE);
    return v;
  }
  // Pick random neighbour
  inline node_t* random_neighbour(const node_t v) { return H_random(neigh[v],deg[v]); }
#if 0
  // Depth-first search.
  node_t depth_search(bool *visited, node_t *buff, node_t v0=0);
  // Returns complexity of isolation test
  long effective_isolated(node_t v, node_t K, node_t *Kbuff, bool *visited);
  // Depth-Exploration. Returns number of steps done. Stops when encounter vertex of degree > dmax.
  void depth_isolated(node_t v, long &calls, int &left_to_explore, int dmax, int * &Kbuff, bool *visited);
#endif

public:
  //degree of v
  inline degree_t degree(const node_t v) { return deg[v]; };
  // For debug purposes : verify validity of the graph (symetry, simplicity)
  bool verify();
  // Destroy deg[], neigh[] and links[]
  ~graph_molloy_hash();
  // Allocate memory for the graph. Create deg and links. No edge is created.
  graph_molloy_hash(degree_sequence &);
  // Create graph from hard copy
  graph_molloy_hash(node_t *);
  // Create hard copy of graph
  node_t *hard_copy();
  // Restore from backup
  void restore(node_t * back);
  //Clear hash tables
  void init();
  // nb arcs
  inline edgeid_t nbarcs() { return a; };
  // nb vertices
  inline node_t nbvertices() { return n; };
  // print graph in SUCC_LIST mode, in stdout
  void print(FILE *f = stdout);
  // Test if graph is connected
  bool is_connected();
  // is edge ?
  inline bool is_edge(node_t a, node_t b) {
    assert(H_is(neigh[a],deg[a],b) == (fast_search(neigh[a],HASH_SIZE(deg[a]),b)!=NULL));
    assert(H_is(neigh[b],deg[b],a) == (fast_search(neigh[b],HASH_SIZE(deg[b]),a)!=NULL));
    assert(H_is(neigh[a],deg[a],b) == H_is(neigh[b],deg[b],a));
    if(deg[a]<deg[b]) return H_is(neigh[a],deg[a],b);
    else return H_is(neigh[b],deg[b],a);
  }
  // Random edge swap ATTEMPT. Return 1 if attempt was a succes, 0 otherwise
  int random_edge_swap(int K=0, node_t *Kbuff=NULL, bool *visited=NULL);
  // Connected Shuffle
  edgeid_t shuffle(edgeid_t , int type);
#if 0  
  // Optimal window for the gkantsidis heuristics
  int optimal_window();
  // Average unitary cost per post-validated edge swap, for some window
  double average_cost(int T, int *back, double min_cost);
  // Get caracteristic K
  double eval_K(int quality = 100);
  // Get effective K
  double effective_K(int K, int quality = 10000);
  // Try to shuffle T times. Return true if at the end, the graph was still connected.
  bool try_shuffle(int T, int K, int *back=NULL);
#endif

/*_____________________________________________________________________________
  Not to use anymore : use graph_molloy_opt class instead

private:
  // breadth-first search. Store the distance (modulo 3)  in dist[]. Returns eplorated component size.
  int width_search(unsigned char *dist, int *buff, int v0=0);

public:
  // Create graph
  graph_molloy_hash(FILE *f);
  // Bind the graph avoiding multiple edges or self-edges (return false if fail)
  bool havelhakimi();
  // Get the graph connected  (return false if fail)
  bool make_connected();
  // "Fab" Shuffle (Optimized heuristic of Gkantsidis algo.)
  long long fab_connected_shuffle(long long);
  // Naive Shuffle
  long long slow_connected_shuffle(long long);
  // Maximum degree
  int max_degree();
  // compute vertex betweenness : for each vertex, a unique random shortest path is chosen.
  // this choice is consistent (if shortest path from a to c goes through b and then d,
  // then shortest path from a to d goes through b). If(trivial path), also count all the
  // shortest paths where vertex is an extremity
  int *vertex_betweenness_rsp(bool trivial_path);
  // same, but when multiple shortest path are possible, average the weights.
  double *vertex_betweenness_asp(bool trivial_path);
//___________________________________________________________________________________
//*/

};

#endif //GRAPH_MOLLOY_HASH_H

