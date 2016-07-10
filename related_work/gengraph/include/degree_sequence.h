#ifndef DEGREE_SEQUENCE_H
#define DEGREE_SEQUENCE_H

#include <stdint.h>

typedef int32_t node_t;
typedef int32_t degree_t;
typedef int64_t edgeid_t;

class degree_sequence {

private:
  node_t n;
  degree_t *deg;
  edgeid_t total;

public :
  // #vertices
  inline node_t size() { return n; };
  inline edgeid_t sum() { return total; };
  inline degree_t operator[](int i) { return deg[i]; };
  inline degree_t *seq() { return deg; };
  inline void assign(int n0, int* d0) { n=n0; deg=d0; };
  inline degree_t dmax() {
    degree_t dm = deg[0];
    for(node_t i=1; i<n; i++)
      if(deg[i]>dm) dm=deg[i];
    return dm;
  }

  void make_even(degree_t mini=-1, degree_t maxi=-1);
  void sort();
  void shuffle();
  
  // raw constructor
  degree_sequence(node_t n, degree_t *degs);

  // read-from-file constrictor
  degree_sequence(FILE *f, bool DISTRIB=true);

  // simple power-law constructor : Pk = int((x+k0)^(-exp),x=k..k+1), with k0 so that avg(X)=z
  degree_sequence(node_t n, double exp, degree_t degmin, degree_t degmax, double avg_degree=-1.0);

  // destructor
  ~degree_sequence();

  // unbind the deg[] vector (so that it doesn't get deleted when the class is destroyed)
  void detach();

  // compute total number of arcs
  void compute_total();
  
  // raw print (vertex by vertex)
  void print();

  // distribution print (degree frequency)
  void print_cumul();

  // is degree sequence realizable ?
  bool havelhakimi();

};

#endif //DEGREE_SEQUENCE_H

