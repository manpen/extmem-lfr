#!/usr/bin/python3

from networkit import *
import os
import tempfile
import subprocess
import random

current_directory = os.path.dirname(os.path.abspath(__file__))

def clusterInfomap(G):
    with tempfile.TemporaryDirectory() as tempdir:
        graph_filename = os.path.join(tempdir, "network.txt")
        graphio.writeGraph(G, graph_filename, fileformat=graphio.Format.EdgeListSpaceZero)
        subprocess.call(["{}/../related_work/infomap/Infomap".format(current_directory), "-s", str(random.randint(-2**31, 2**31)), "-2", "-z", "--clu", graph_filename, tempdir])
        print("Reading infomap")
        result = community.readCommunities(os.path.join(tempdir, "network.clu"), format="edgelist-s0")
        while result.numberOfElements() < G.upperNodeIdBound():
            result.toSingleton(result.extend())

        return result

def genOrigLFR(N, k=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100, on=0, om=0, C=None):
    # FIXME use minimum instead of average
    args = ["{}/../related_work/binary_networks/benchmark".format(current_directory), "-N", N, "-k", k, "-maxk", maxk, "-mu", mu, "-t1", -1 * t1, "-t2", -1 * t2, "-minc", minc, "-maxc", maxc]
    if on > 0:
        args.extend(["-on", on, "-om", om])
    if C is not None:
        args.extend(["-C", C])

    with tempfile.TemporaryDirectory() as tempdir:
        old_dir = os.getcwd()
        try:
            os.chdir(tempdir)
            subprocess.call(map(str, args))
        finally:
            os.chdir(old_dir)

        G = graphio.readGraph(os.path.join(tempdir, "network.dat"), fileformat=graphio.Format.LFR)
        if on == 0:
            C = community.readCommunities(os.path.join(tempdir, "community.dat"), format='edgelist-t1')
            print("finished reading orig lfr")
        else:
            C = graphio.EdgeListCoverReader(1).read(os.path.join(tempdir, "community.dat"), G)
        return (G, C)

def genEMLFR(N, mink=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100, on=0, om=0):
    with tempfile.TemporaryDirectory() as tempdir:
        network_filename = os.path.join(tempdir, "foo.metis.graph")
        partition_filename = os.path.join(tempdir, "foo.part")
        args = ["{}/../release/pa_lfr".format(current_directory), "-n", N, "-c", int(N/minc), "-i", mink, "-a", maxk, "-m", mu, "-j", t1, "-z", t2, "-x", minc, "-y", maxc, "-o", network_filename, "-p", partition_filename]
        if on > 0:
            args.extend(["-l", on, "-k", om])

        subprocess.call(map(str, args))

        G = graphio.readGraph(network_filename, fileformat=graphio.Format.METIS)

        if on == 0:
            C = community.readCommunities(partition_filename, format='edgelist-s0')
        else:
            C = graphio.EdgeListCoverReader(0).read(partition_filename)

        return (G, C)

def genNetworKitLFR(N, mink=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100):
    gen = generators.LFRGenerator(N)

    pl = generators.PowerlawDegreeSequence(mink, maxk, t1)
    pl.run()
    gen.setDegreeSequence(pl.getDegreeSequence(N))

    gen.generatePowerlawCommunitySizeSequence(minc, maxc, t2)
    gen.setMu(mu)
    gen.run()
    return (gen.getGraph(), gen.getPartition())

minDeg = 10
minCom = 20
degExp = -2
comExp = -1

with open('cluster_stats.csv', 'a') as outf:
    outf.write("n, minDeg, maxDeg, degExp, minCom, maxCom, comExp, mu, Generator, m, ComAlg, Comp, score, run\n")

    for run in range(10):
        for n in (10**i for i in range(3, 7)):
            maxDeg = maxCom = int(n / 20)

            avgDeg = generators.PowerlawDegreeSequence(minDeg, maxDeg, degExp).run().getExpectedAverageDegree()

            for mu in (m / 10 for m in range(2, 7, 2)):
                print(n, maxDeg, mu)

                for gen in ["Orig", "EM", "NetworKit"]:
                    if gen == "Orig":
                        (G, P) = genOrigLFR(n, avgDeg, maxDeg, mu=mu, t1=degExp, t2=comExp, minc=minCom, maxc=maxCom)
                    elif gen == "EM":
                        (G, P) = genEMLFR(n, minDeg, maxDeg, mu=mu, t1=degExp, t2=comExp, minc=minCom, maxc=maxCom)
                    elif gen == "NetworKit":
                        (G, P) = genNetworKitLFR(n, minDeg, maxDeg, mu=mu, t1=degExp, t2=comExp, minc=minCom, maxc=maxCom)
                    else:
                        raise RuntimeError("wrong generator name!")

                    for alg in ["Infomap", "Louvain"]:
                        if alg == "Infomap":
                            foundP = clusterInfomap(G)
                        elif alg == "Louvain":
                            foundP = community.PLM(G, par="none randomized").run().getPartition()
                        else:
                            raise RuntimeError("wrong algorithm name!")

                        for compAlg, compName in [(community.NMIDistance(), "NMI"), (community.AdjustedRandMeasure(), "AR")]:
                            score = 1.0 - compAlg.getDissimilarity(G, foundP, P)

                            outf.write(", ".join(map(str, [n, minDeg, maxDeg, degExp, minCom, maxCom, comExp, mu, G.numberOfEdges(), gen, alg, compName, score, run])))
                            outf.write("\n")

                        outf.flush()
