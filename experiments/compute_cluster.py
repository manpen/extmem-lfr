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
        if ovlFac == 1:
            C = community.readCommunities(cpath, format='edgelist-t1')
            print("finished reading orig lfr")
        else:
            C = graphio.EdgeListCoverReader(1).read(cpath, G)
        
    elif (gen == "NetworKit" or gen == "EM"):
        assert("metis.graph" in path)
        G = graphio.readGraph(path, fileformat=graphio.Format.METIS)
        bpath = path.replace(".metis.graph", "")
        cpath = bpath + ".part"

        if ovlFac == 1:
            C = community.readCommunities(cpath, format='edgelist-s0')
        else:
            C = graphio.EdgeListCoverReader(0).read(cpath, G)

    return (G, C, bpath, gen, n, minDeg, maxDeg, mu, minCom, maxCom, ovlNodes, ovlFac) 

def processFile(path):
    afile = path.replace(".metis.graph", "").replace(".network.dat", "") + ".analysis"
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

        for alg in ["Infomap", "Louvain"]:
            with Walltime(alg):
               if alg == "Infomap":
                   with open(afile + "infomap", 'w') as log_outf:
                       foundP = clusterInfomap(G, log_outf)
               elif alg == "Louvain":
                   if (ovlFac > 1):
                       print("Overlapping comm not supported; skip")
                       continue
                   foundP = community.PLM(G, par="none randomized").run().getPartition()
               else:
                   raise RuntimeError("wrong algorithm name!")

            for compAlg, compName in [(community.NMIDistance(), "NMI"), (community.AdjustedRandMeasure(), "AR")]:
                if (ovlFac > 1 and compName == "AR"): continue
                score = 1.0 - compAlg.getDissimilarity(G, foundP, P)

                outf.write(", ".join(map(str, pref + [alg, compName, score])) + "\n")

        avgcc = globals.ClusteringCoefficient().avgLocal(G, turbo=True)
        outf.write(", ".join(map(str, pref + ["AvgCC", '', avgcc]))+"\n")

    return True


networks = []
networks += glob.glob("/home/memhierarchy/penschuck/scratch/networks_*/proc*/*of1-*.metis.graph")
networks += glob.glob("/home/memhierarchy/penschuck/scratch/networks_*/proc*/*of1*.network.dat")

random.shuffle(networks)
for x in networks:
    print(x)
    try:
        res = processFile(x)
        print("Ok" if res else "Skipped")
    except Exception as s:
        print("Err " + str(s))
    print("\n"*5)
