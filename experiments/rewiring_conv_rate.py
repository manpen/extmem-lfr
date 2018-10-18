#!/usr/bin/env python3
import fileinput
import numpy as np


R_DUPS=0
R_RNDS=1
R_COM_W_DUP=2
R_COM_W_RND=3
R_NO_COMS=4
R_NO_EDGES=5

iters=[]

for line in fileinput.input():
    data = [int(x) for x in line.split(" ")]
    it = data[0]
    while len(iters) < it:
        iters.append([])
    iters[-1].append(data[1:])
    
maxPoints = max((len(x) for x in iters))
print("Found at most %d data points" % maxPoints)

first_below = [len(iters) + 10] * 5

for it in range(1, len(iters)+1):
    field = np.zeros( (maxPoints, 6) )
    field[:, R_NO_EDGES] = 1
    field[:len(iters[it-1]),:] = iters[it-1]
    
    conv = 1.0 * field[:, R_DUPS] / field[:, R_NO_EDGES]
    conv.sort()

    for x in range(5):
        if conv[int(0.25 * (maxPoints-1) * x)] < 1e-3:
            first_below[x] = min(first_below[x], it)
    
    bp = " ".join( (str(conv[int((maxPoints-1) / 4.0 * i)]) for i in range(5)) )
    
    print(str(it) + " " + bp)
    
print("# below: " + " ".join(map(str, first_below)))
