from networkit import *
import os
import tempfile
import subprocess
import random
import timeit
import socket

sysTempDir = None
if ('SLURM_JOB_ID' in os.environ):
   # presumably we're running on LOEWE CSC
   sysTempDir = '/local/' + str(int(os.environ['SLURM_JOB_ID'])) + '/'
   print("Using SLURM-aware temp. directory: " + sysTempDir)

print("HOST: " + socket.gethostname())

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

def genOrigLFR(N, mink=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100, on=0, om=0, C=None, outf=subprocess.STDOUT):
    # FIXME use minimum instead of average
    args = ["/usr/bin/time", "-av", "{}/../related_work/binary_networks/benchmark".format(current_directory), "-N", N, "-mink", mink, "-maxk", maxk, "-mu", mu, "-t1", -1 * t1, "-t2", -1 * t2, "-minc", minc, "-maxc", maxc]
    if on > 0:
        args.extend(["-on", on, "-om", om])
    if C is not None:
        args.extend(["-C", C])

    with tempfile.TemporaryDirectory(dir=sysTempDir) as tempdir:
        old_dir = os.getcwd()
        try:
            os.chdir(tempdir)
            with Walltime("OrigLFR"):
               subprocess.call(map(str, args), stdout=outf, stderr=outf)
        finally:
            os.chdir(old_dir)

        G = graphio.readGraph(os.path.join(tempdir, "network.dat"), fileformat=graphio.Format.LFR)
        if on == 0:
            C = community.readCommunities(os.path.join(tempdir, "community.dat"), format='edgelist-t1')
            print("finished reading orig lfr")
        else:
            C = graphio.EdgeListCoverReader(1).read(os.path.join(tempdir, "community.dat"), G)
        return (G, C)

def genEMLFR(N, mink=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100, on=0, om=0, outf=subprocess.STDOUT):
    with tempfile.TemporaryDirectory(dir=sysTempDir) as tempdir:
        network_filename = os.path.join(tempdir, "foo.metis.graph")
        partition_filename = os.path.join(tempdir, "foo.part")
        args = ["/usr/bin/time", "-av", "{}/../release/pa_lfr".format(current_directory), "-n", N, "-c", int(N/minc), "-i", mink, "-a", maxk, "-m", mu, "-j", t1, "-z", t2, "-x", minc, "-y", maxc, "-o", network_filename, "-p", partition_filename, "-b", "15Gi"]
        if on > 0:
            args.extend(["-l", on, "-k", om])

        with Walltime("EMLFR"):
           subprocess.call(map(str, args), stdout=outf, stderr=outf)

        G = graphio.readGraph(network_filename, fileformat=graphio.Format.METIS)

        if on == 0:
            C = community.readCommunities(partition_filename, format='edgelist-s0')
        else:
            C = graphio.EdgeListCoverReader(0).read(partition_filename)

        return (G, C)

def genNetworKitLFR(N, mink=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100):
    with Walltime("NetworkitLFR"):
       while True:
           try: # we might need to try several times because depending on the random values NetworKit cannot generate it
               gen = generators.LFRGenerator(N)

               pl = generators.PowerlawDegreeSequence(mink, maxk, t1)
               pl.run()
               gen.setDegreeSequence(pl.getDegreeSequence(N))

               gen.generatePowerlawCommunitySizeSequence(minc, maxc, t2)
               gen.setMu(mu)
               gen.run()
           except RuntimeError:
               pass

           if gen.hasFinished():
               break

    return (gen.getGraph(), gen.getPartition())

minDeg = 10
minCom = 20
degExp = -2
comExp = -1
ovlNodes = 0
ovlFac = 0

logdir = "./"
logfile = "cluster_stats.csv"
if ('SLURM_PROCID' in os.environ):
	logdir += "proc%d/" % int(os.environ['SLURM_PROCID'])
	os.mkdir(logdir)
	logfile = "cluster_stats_%d_%d.csv" % (int(os.environ['SLURM_JOB_ID']), int(os.environ['SLURM_PROCID']))

with open(logdir+logfile, 'a') as outf:
    outf.write("n, minDeg, maxDeg, degExp, minCom, maxCom, comExp, mu, Generator, m, ComAlg, Comp, score, run\n")

    for run in range(1):
        for n in (10**i for i in range(3,9)):
            maxDeg = maxCom = int(n / 20)

            avgDeg = generators.PowerlawDegreeSequence(minDeg, maxDeg, degExp).run().getExpectedAverageDegree()

            for mu in (m / 10 for m in range(2, 7, 2)):
                print(n, maxDeg, mu)

                for gen in ["Orig", "NetworKit"]:
                    print("-" * 100)
                    print("Generate. Algo: %s, minDeg: %d\tmaxDeg: %d\tmu:%.2f\tminCom: %d maxCom:%d" % (gen, minDeg, maxDeg, mu, minCom, maxCom))
                    label="%s_n%d_kmin%d_kmax%d_mu%d_minc%d_maxc%d_on%d_of%d-%d" % (gen,n,minDeg,maxDeg,int(mu*10), minCom, maxCom, ovlNodes, ovlFac, run)
                    with open(logdir + "gen_" + label, 'w') as log_outf:
                        if gen == "Orig":
                            (G, P) = genOrigLFR(n, minDeg, maxDeg, mu=mu, t1=degExp, t2=comExp, minc=minCom, maxc=maxCom, outf=log_outf)
                        elif gen == "EM":
                            (G, P) = genEMLFR(n, minDeg, maxDeg, mu=mu, t1=degExp, t2=comExp, minc=minCom, maxc=maxCom, outf=log_outf)
                        elif gen == "NetworKit":
                            if (ovlNodes > 0):
                                print("Overlapping comm not supported; skip")
                                continue
                            (G, P) = genNetworKitLFR(n, minDeg, maxDeg, mu=mu, t1=degExp, t2=comExp, minc=minCom, maxc=maxCom)
                        else:
                            raise RuntimeError("wrong generator name!")

                    for alg in ["Infomap", "Louvain"]:
                        with Walltime(alg):
                           if alg == "Infomap":
                               with open(logdir + "infomap_" + label, 'w') as log_outf:
                                   foundP = clusterInfomap(G, log_outf)
                           elif alg == "Louvain":
                               if (ovlNodes > 0):
                                   print("Overlapping comm not supported; skip")
                                   continue
                               foundP = community.PLM(G, par="none randomized").run().getPartition()
                           else:
                               raise RuntimeError("wrong algorithm name!")

                        for compAlg, compName in [(community.NMIDistance(), "NMI"), (community.AdjustedRandMeasure(), "AR")]:
                            if (ovlNodes > 0 and compName == "AR"): continue
                            score = 1.0 - compAlg.getDissimilarity(G, foundP, P)

                            outf.write(", ".join(map(str, [n, minDeg, maxDeg, degExp, minCom, maxCom, comExp, mu, G.numberOfEdges(), gen, alg, compName, score, run])))
                            outf.write("\n")

                        outf.flush()

                    avgcc = globals.ClusteringCoefficient().avgLocal(G, turbo=True)
                    outf.write(", ".join(map(str, [n, minDeg, maxDeg, degExp, minCom, maxCom, comExp, mu, G.numberOfEdges(), gen, "AvgCC", '', avgcc, run])))
                    outf.write("\n")
