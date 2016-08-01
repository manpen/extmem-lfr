#!/usr/bin/env python3
import numpy as np
import sys

assert(len(sys.argv))
file = sys.argv[1]
ofile = sys.argv[2]


def filter_lines(fname, label):
   with open(fname) as f:
      for line in (str(x).rstrip('\n') for x in f):
         if line.endswith(label):
            yield line.split("#")[0]


mat_free = np.loadtxt(filter_lines(file, "free"))
mat_hitsl = np.loadtxt(filter_lines(file, "hitsl"))
mat_hitsu = np.loadtxt(filter_lines(file, "hitsu"))

assert(mat_free.shape == mat_hitsl.shape)
assert(mat_free.shape == mat_hitsu.shape)

mats = [mat_free, mat_hitsl, mat_hitsu]

mat_result = np.zeros( (mat_free.shape[1], 3*len(mats)+1) )
mat_result[:,0] = range(1, mat_result.shape[0]+1)
for i in range(len(mats)):
   mats[i] = mats[i][:10, :]
   mat_result[:, 3*i+1]  = np.average(mats[i], axis=0)
   mat_result[:, 3*i+2]  = mat_result[:, 3*i+1] / np.sum(mat_result[:, 3*i+1])
   mat_result[:, 3*i+3]  = np.std(mats[i], axis=0) / np.sum(mat_result[:, 3*i+1])

np.savetxt(ofile, mat_result, fmt="%d " + ("%f %f %f    "*3))