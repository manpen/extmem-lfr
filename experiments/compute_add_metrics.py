#!/usr/bin/env python3

from networkit import *
import os
import tempfile
import subprocess
import random
import timeit
import socket
import glob
import re
import numpy as np

print("HOST: " + socket.gethostname())
sysTempDir = None
if ('SLURM_JOB_ID' in os.environ):
   # presumably we're running on LOEWE CSC
   sysTempDir = '/local/' + str(int(os.environ['SLURM_JOB_ID'])) + '/'
   print("Using SLURM-aware temp. directory: " + sysTempDir)
   os.environ["OMP_NUM_THREADS"] = "10"


class Walltime:
   def __init__(self, label):
      self.label = label

   def __enter__(self):
      self.start_time = timeit.default_timer()

   def __exit__(self, a,b,c):
      print("%s: Runtime %f s" % (self.label, timeit.default_timer() - self.start_time))

current_directory = os.path.dirname(os.path.abspath(__file__))

def clusterInfomap(G, outf=subprocess.STDOUT):
    with tempfile.TemporaryDirectory(dir=sysTempDir) as tempdir:
        graph_filename = os.path.join(tempdir, "network.txt")
        graphio.writeGraph(G, graph_filename, fileformat=graphio.Format.EdgeListSpaceZero)
        subprocess.call(["/usr/bin/time", "-av", "{}/../related_work/infomap/Infomap".format(current_directory), "-s", str(random.randint(-2**31, 2**31)), "-2", "-z", "--clu", graph_filename, tempdir], stdout=outf, stderr=outf)
        print("Reading infomap")
        result = community.readCommunities(os.path.join(tempdir, "network.clu"), format="edgelist-s0")
        while result.numberOfElements() < G.upperNodeIdBound():
            result.toSingleton(result.extend())

        return result

def loadGraph(path):
    G = None
    C = None

    x = re.match(r".*/(EM|Orig|NetworKit)_n(\d+)_kmin(\d+)_kmax(\d+)_mu(\d)_minc(\d+)_maxc(\d+)_on(\d+)_of(\d+)-", path)
    assert(x)
    gen,n,minDeg,maxDeg,mu, minCom, maxCom, ovlNodes, ovlFac = x.groups()

    n=int(n)
    minDeg=int(minDeg)
    maxDeg=int(maxDeg)
    minCom=int(minCom)
    maxCom=int(maxCom)
    ovlNodes=int(ovlNodes)
    ovlFac=int(ovlFac)
    mu = int(mu) / 10.0

    bpath = ""

    if (gen == "Orig"):
        assert("network.dat" in path)
        G = graphio.readGraph(path, fileformat=graphio.Format.LFR)
        bpath = path.replace('.network.dat', '')
        cpath = bpath + '.comm.dat'
        C = graphio.EdgeListCoverReader(1).read(cpath, G)
        
    elif (gen == "NetworKit" or gen == "EM"):
        assert("metis.graph" in path)
        G = graphio.readGraph(path, fileformat=graphio.Format.METIS)
        bpath = path.replace(".metis.graph", "")
        cpath = bpath + ".part"
        C = graphio.EdgeListCoverReader(0).read(cpath, G)

    return (G, C, bpath, gen, n, minDeg, maxDeg, mu, minCom, maxCom, ovlNodes, ovlFac) 

def processFile(path):
    afile = path.replace(".metis.graph", "").replace(".network.dat", "") + ".analysis1"
    if os.path.isfile(afile):
        return False

    degExp=-2
    comExp=-2
    run=0
    G, P, bpath, gen,n,minDeg,maxDeg,mu, minCom, maxCom, ovlNodes, ovlFac = loadGraph(path)

    if os.path.isfile(afile):
        return False

    # here we might get a race condition, but worst case is, we do the work twice
    with open(afile, 'w') as outf:
        x = re.match(r".*/networks_(\d+)/proc(\d+)/.+of\d+-(\d+)\.", path)
        if x:
            run = '"' + "-".join(x.groups()) + '"'

        pref = [n, minDeg, maxDeg, degExp, minCom, maxCom, comExp, mu, ovlNodes, ovlFac, G.numberOfEdges(), gen, run]

        assort = computeAssort(G)
        outf.write(", ".join(map(str, pref + ["Assort", '', assort]))+"\n")


        ginis = computeGini(G, P)
        for g in ginis:
            outf.write(", ".join(map(str, pref + ["Gini", g[0], g[1]]))+"\n")

    return True

def giniCoeff(array):
    """Calculate the Gini coefficient of a numpy array."""
    # based on bottom eq: http://www.statsdirect.com/help/content/image/stat0206_wmf.gif
    # from: http://www.statsdirect.com/help/default.htm#nonparametric_methods/gini.htm
    array = array.flatten() #all values are treated equally, arrays must be 1d
    if np.amin(array) < 0:
        array -= np.amin(array) #values cannot be negative
    array += 0.0000001 #values cannot be 0
    array = np.sort(array) #values must be sorted
    index = np.arange(1,array.shape[0]+1) #index per array element
    n = array.shape[0]#number of array elements
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array))) #Gini coefficient


def computeGini(graph, cover):
    comms = []
    for subsetId in cover.getSubsetIds():
        degs = np.array([graph.degree(node)*1.0 for node in cover.getMembers(subsetId)])
        gini = giniCoeff(degs)
        size = len(cover.getMembers(subsetId))

        comms.append([size, gini])

    return comms

def computeAssort(graph):
    degs = [graph.degree(x) for x in graph.nodes()]
    return correlation.Assortativity(graph, degs).run().getCoefficient()


networks = []
networks += glob.glob("/mnt/data/manpen/networks_357700*/proc*/*.network.dat")

random.shuffle(networks)
for x in networks:
    print(x)
    try:
        res = processFile(x)
        print("Ok" if res else "Skipped")
    except Exception as s:
        print("Err " + str(s))
    print("\n"*5)
