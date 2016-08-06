#!/usr/bin/env python3
import os, sys
import tempfile
import subprocess
import random
import timeit
import socket
import shutil
from networkit import *

sysTempDir = None
if ('SLURM_JOB_ID' in os.environ):
   # presumably we're running on LOEWE CSC
   sysTempDir = '/local/' + str(int(os.environ['SLURM_JOB_ID'])) + '/'
   print("Using SLURM-aware temp. directory: " + sysTempDir)
   os.environ["OMP_NUM_THREADS"] = "8"

gens=[]
for x in sys.argv:
	if "-o" == x:
		print("Added Orig LFR")
		gens.append("Orig")
	if "-e" == x:
		print("Added EM LFR")
		gens.append("EM")
	if "-n" == x:
		print("Added NetworKit")
		gens.append("NetworKit")

gens = list(set(gens))
assert(len(gens))

print("HOST: " + socket.gethostname())

class Walltime:
   def __init__(self, label):
      self.label = label

   def __enter__(self):
      self.start_time = timeit.default_timer()

   def __exit__(self, a,b,c):
      print("%s: Runtime %f s" % (self.label, timeit.default_timer() - self.start_time))

current_directory = os.path.dirname(os.path.abspath(__file__))

def genOrigLFR(N, mink=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100, on=0, om=0, C=None, outf=subprocess.STDOUT, outFn=None):
    args = ["/usr/bin/time", "-av", "{}/../related_work/binary_networks/benchmark".format(current_directory), "-N", N, "-mink", mink, "-maxk", maxk, "-mu", mu, "-t1", -1 * t1, "-t2", -1 * t2, "-minc", minc, "-maxc", maxc]
    if om > 1:
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

        if (outFn):
            shutil.copyfile(os.path.join(tempdir, "network.dat"), outFn + ".network.dat")
            shutil.copyfile(os.path.join(tempdir, "community.dat"), outFn + ".comm.dat")

def genEMLFR(N, mink=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100, on=0, om=0, outf=subprocess.STDOUT, outFn=None):
    with tempfile.TemporaryDirectory(dir=sysTempDir) as tempdir:
        network_filename = os.path.join(tempdir, "foo.metis.graph")
        partition_filename = os.path.join(tempdir, "foo.part")
        if outFn:
            network_filename = outFn + ".metis.graph"
            partition_filename = outFn + ".part"

        args = ["/usr/bin/time", "-av", "{}/../release/pa_lfr".format(current_directory), "-n", N, "-c", int(N/minc), "-i", mink, "-a", maxk, "-m", mu, "-j", t1, "-z", t2, "-x", minc, "-y", maxc, "-o", network_filename, "-p", partition_filename, "-b", "6Gi"]
        if on > 0:
            args.extend(["-l", on, "-k", om])

        with Walltime("EMLFR"):
           subprocess.call(map(str, args), stdout=outf, stderr=outf)

def genNetworKitLFR(N, mink=20, maxk=50, mu=0.3, t1=-2, t2=-1, minc=20, maxc=100, outFn=""):
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
    
    community.writeCommunities(gen.getPartition(), outFn + ".part")
    graphio.METISGraphWriter().write(gen.getGraph(), outFn + ".metis.graph")


ovlFac = 1

logdir = "./"
logfile = "cluster_stats.csv"
if ('SLURM_PROCID' in os.environ):
        logdir = "/scratch/memhierarchy/penschuck/networks_%d/proc%d/" % (int(os.environ['SLURM_JOB_ID']), int(os.environ['SLURM_PROCID']))
        if not os.path.exists(logdir):
              os.mkdir(logdir)	

        logfile = "cluster_stats_%d_%d.csv" % (int(os.environ['SLURM_JOB_ID']), int(os.environ['SLURM_PROCID']))

        ovlFac = 1 + (int(os.environ['SLURM_PROCID']) % 5)
        if (len(gens) == 1 and gens[0] == "NetworKit"):
              ovlFac = 1

minDeg = 10
minCom = 20 * ovlFac
degExp = -2
comExp = -1

decs = 1 
stepsPerDec = 3
n0 = 10000000

with open(logdir+logfile, 'a') as outf:
    outf.write("n, minDeg, maxDeg, degExp, minCom, maxCom, comExp, mu, Generator, m, ComAlg, Comp, score, run\n")

    for run in range(5):
        for n in (int(n0 * 10**(i/float(stepsPerDec))) for i in range(0,decs*stepsPerDec + 1)):
            maxDeg = maxCom = int(n / 20)
            maxCom *= ovlFac
            ovlNodes=n

            for mu in (m / 10 for m in range(2, 7, 2)):
                print(n, maxDeg, mu)

                for gen in gens:
                    print("-" * 100)
                    print("Generate. Algo: %s, minDeg: %d\tmaxDeg: %d\tmu:%.2f\tminCom: %d maxCom:%d" % (gen, minDeg, maxDeg, mu, minCom, maxCom))
                    label="%s_n%d_kmin%d_kmax%d_mu%d_minc%d_maxc%d_on%d_of%d-%d" % (gen,n,minDeg,maxDeg,int(mu*10), minCom, maxCom, ovlNodes, ovlFac, run)
                    with open(logdir + "gen_" + label, 'w') as log_outf:
                        if gen == "Orig":
                            genOrigLFR(n, minDeg, maxDeg, mu=mu, on=ovlNodes, om=ovlFac,  t1=degExp, t2=comExp, minc=minCom, maxc=maxCom, outf=log_outf, outFn=logdir + label)
                        elif gen == "EM":
                            genEMLFR(n, minDeg, maxDeg, mu=mu, on=ovlNodes, om=ovlFac, t1=degExp, t2=comExp, minc=minCom, maxc=maxCom, outf=log_outf, outFn=logdir + label)
 
                        elif gen == "NetworKit":
                            if (ovlFac > 1):
                                print("Overlapping comm not supported; skip")
                                continue
                            genNetworKitLFR(n, minDeg, maxDeg, mu=mu, t1=degExp, t2=comExp, minc=minCom, maxc=maxCom, outFn=logdir+label)

                        else:
                            raise RuntimeError("wrong generator name!")
