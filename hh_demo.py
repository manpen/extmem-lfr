#!/usr/bin/env python3
from networkit import *
g = graphio.readGraph("graph.metis", fileformat=graphio.Format.METIS)
overview(g)