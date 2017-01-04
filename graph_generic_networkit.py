#!/usr/bin/env python3
from networkit import *

name = sys.argv[1]

g = graphio.readGraph(name, fileformat=graphio.Format.METIS)
overview(g)
