import argparse
from numpy import random
from Simulator import *
from Listeners import *
from Resource import *
from Machine import *
from scheduling.BaseSchedulers import *
from scheduling.BinPackingScheduler import *
from Generator import *


#---FUNCIONS--------------------------------------------------------------------

def getJobTime(job):
    ops = job.operations
    noCPU_threads = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                                    job.resourceRequest)))
    cpu_cores = min(noCPU_threads, args.NO_CORES)
    cpuTime = job.operations.get(RType.CPU_core, 0) / (cpu_cores * args.CPU_SPEED + 1e-8)
    noGPUs = len(list(filter(lambda r: r.rtype == Resource.Type.GPU,
                                    job.resourceRequest)))
    gpu_cores = min(noGPUs, args.NO_GPUS) * args.N_CC
    gpuTime = job.operations.get(RType.GPU, 0) / (gpu_cores * args.GPU_SPEED + 1e-8)
    return max(cpuTime, gpuTime)


#---ARGUMENT-PARSING------------------------------------------------------------

parser = argparse.ArgumentParser()
parser.add_argument('--seed', dest='SEED', default=-1, type=int)
parser.add_argument('--jobs', dest='NO_JOBS', default=100, type=int)
parser.add_argument('--dist-param', dest='TH_BIN_DIST_PARAM', default=0.1, type=float)
parser.add_argument('--span', dest='SPAN', default=0, type=float,
                    help='maximal time of scheduling tasks')
parser.add_argument('--max-threads', dest='MAX_THREADS', default=-1, type=int)
parser.add_argument('--scheduler', dest='SCHEDULER', default="Simple", type=str,
                    help='options: Simple, BinPacking')
parser.add_argument('--sep-bins', dest='SEP_BINS', action="store_true")
parser.add_argument('--bin-type', dest='BIN_TYPE', default="Simple", type=str,
                    help='options: Simple, Reductive, Timeline, OrderedTimeline')
parser.add_argument('--bin-limit', dest='BIN_TASK_LIMIT', default=-1, type=int,
                    help='max number of tasks per bin. -1 means no limit')
parser.add_argument('--len-tol', dest='LEN_TOL', default=-1, type=float,
                    help='tolerance of different lengths of tasks in bin. -1 means no limit')
parser.add_argument('--pr-param', dest='PP', default=1, type=float)
parser.add_argument('--speed', dest='CPU_SPEED', default=3.6, type=float)
parser.add_argument('--cores', dest='NO_CORES', default=4, type=int)
parser.add_argument('--ram', dest='RAM_SIZE', default=16, type=float)
parser.add_argument('--gpus', dest='NO_GPUS', default=4, type=int)
parser.add_argument('--gpu-speed', dest='GPU_SPEED', default=1.05, type=float)
parser.add_argument('--nCC', dest='N_CC', default=1024, type=int,
                    help='number of cuda cores per GPU')
args = parser.parse_args()

if args.SEED >= 0:
    random.seed(args.SEED)
if args.MAX_THREADS < 0:
    args.MAX_THREADS = args.NO_CORES
if args.BIN_TASK_LIMIT < 0:
    args.BIN_TASK_LIMIT = None
if args.LEN_TOL < 0:
    args.LEN_TOL = None

OrderedTimelineBinLF = OrderedTimelineBinClass(
    orderAdd    = orderLongestFirst,
    orderRemove = orderLongestFirst,
    orderClose  = orderExhausive,
)
BINS = {
    'Simple': SimpleBin,
    'Reductive': ReductiveBin,
    'Timeline': TimelineBin,
    'OrderedTimeline': OrderedTimelineBinLF,
}
Bin = BINS[args.BIN_TYPE]
SCHEDULERS = {
    'Simple': VMSchedulerSimple,
    'BinPacking': lambda *largs, **kwargs: \
        BinPackingScheduler(*largs, **kwargs,
                            BinClass=Bin,
                            awaitBins=args.SEP_BINS,
                            binTasksLimit=args.BIN_TASK_LIMIT,
                            lengthDiffTolerance=args.LEN_TOL),
}
Scheduler = SCHEDULERS[args.SCHEDULER]


#---GET-MACHINE-----------------------------------------------------------------

resources = { Resource(RType.RAM, args.RAM_SIZE) }
for _ in range(args.NO_CORES):
    resources.add(Resource(RType.CPU_core, args.CPU_SPEED))
for _ in range(args.NO_GPUS):
    resources.add(Resource(RType.GPU, args.N_CC, args.GPU_SPEED))
machine = Machine("m0", resources, lambda m: None, Scheduler)


#---GET-JOBS--------------------------------------------------------------------

def priorities(s):
    ''' return list of `s` random functions `int -> int` '''
    a_s = np.abs(np.random.normal(1, 0.3*args.PP, s))/100
    b_s = np.abs(np.random.normal(1, 1.0*args.PP, s))
    return [(lambda t, a=a, b=b: a*t + b) for a, b in zip(a_s, b_s)]

gen = RandomJobGenerator(
    noCores=lambda s: 1 + random.binomial(
            args.MAX_THREADS-1,
            args.TH_BIN_DIST_PARAM,
            s,
    ),
    priorities=priorities,
)
jobs = gen.getJobs(args.NO_JOBS)


#---SCHEDULE--------------------------------------------------------------------

seqTime = 0
totalOps = {}
vms = []
for job in jobs:
    seqTime += getJobTime(job)
    totalOps = dictPlus(totalOps, job.operations)
    vm = CreateVM.minimal([job], ownCores=True)
    vm.scheduleJob(job)
    #  machine.scheduleVM(vm)
    vms += [vm]

delayScheduler = VMDelayScheduler(machine,
        lambda n: np.random.uniform(0, args.SPAN, n))
delayScheduler.scheduleVM(vms)


#---RUN-------------------------------------------------------------------------

metric = AUPMetric()
eff_calc = EfficiencyCalculator()
sim = Simulator.getInstance()
sim.simulate()

assert machine._vmScheduler.noVMsLeft == 0

thCPUBestTime = totalOps.get(RType.CPU_core, 0) / (args.NO_CORES * args.CPU_SPEED)
thGPUBestTime = totalOps.get(RType.GPU, 0) / (args.NO_GPUS * args.N_CC * args.GPU_SPEED)
print("simulation time:               ", sim.time)
print("sequence execution time:       ", seqTime)
print("theoretical best possible time:", max(thCPUBestTime, thGPUBestTime))
print("priority cost:                 ", metric.cost(args.SPAN == 0))
for m, d in eff_calc.get().items():
    print(d)

