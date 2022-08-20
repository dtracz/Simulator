import argparse
from numpy import random
from Simulator import *
from Resource import *
from Machine import *
from scheduling.BaseSchedulers import *
from scheduling.BinPackingScheduler import *
from Generator import *


parser = argparse.ArgumentParser()
parser.add_argument('--speed', dest='CPU_SPEED', default=3.6, type=float)
parser.add_argument('--cores', dest='NO_CORES', default=4, type=int)
parser.add_argument('--ram', dest='RAM_SIZE', default=16, type=float)
parser.add_argument('--jobs', dest='NO_JOBS', default=100, type=int)
parser.add_argument('--dist-param', dest='TH_BIN_DIST_PARAM', default=0.1, type=float)
parser.add_argument('--max-threads', dest='MAX_THREADS', default=-1, type=int)
parser.add_argument('--seed', dest='SEED', default=-1, type=int)
parser.add_argument('--scheduler', dest='SCHEDULER', default="Simple", type=str,
                    help='options: Simple, BinPacking')
parser.add_argument('--sep-bins', dest='SEP_BINS', action="store_true")
parser.add_argument('--bin-type', dest='BIN_TYPE', default="Simple", type=str,
                    help='options: Simple, Reductive, Timeline, OrderedTimeline')
args = parser.parse_args()

if args.SEED >= 0:
    random.seed(args.SEED)

if args.MAX_THREADS < 0:
    args.MAX_THREADS = args.NO_CORES

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
        BinPackingScheduler(*largs, **kwargs, BinClass=Bin, awaitBins=args.SEP_BINS),
}
Scheduler = SCHEDULERS[args.SCHEDULER]



resources = { Resource(RType.RAM, args.RAM_SIZE) }
for _ in range(args.NO_CORES):
    resources.add(Resource(RType.CPU_core, args.CPU_SPEED))
machine = Machine("m0", resources, lambda m: None, Scheduler)

gen = RandomJobGenerator(
    noCores=lambda s: 1 + random.binomial(
            args.MAX_THREADS-1,
            args.TH_BIN_DIST_PARAM,
            s,
    )
)
jobs = gen.getJobs(args.NO_JOBS)

totalOps = 0
theoreticalTotalTime = 0
for job in jobs:
    ops = job.operations
    noThreads = len(list(filter(lambda r: r.rtype == RType.CPU_core,
                                job.resourceRequest)))
    totalOps += ops
    theoreticalTotalTime += ops / (min(noThreads, args.NO_CORES) * args.CPU_SPEED)
    vm = CreateVM.minimal([job], ownCores=True)
    vm.scheduleJob(job)
    machine.scheduleVM(vm)

sim = Simulator.getInstance()
sim.simulate()

assert machine._vmScheduler.vmsLeft == 0

print("simulation time:               ", sim.time)
print("sequence execution time:       ", theoreticalTotalTime)
print("theoretical best possible time:", totalOps / (args.CPU_SPEED * args.NO_CORES))

