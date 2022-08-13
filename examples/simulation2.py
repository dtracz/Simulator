import argparse
import numpy as np
from Simulator import *
from Resource import *
from Machine import *
from scheduling.BaseSchedulers import *
from scheduling.BinPackingScheduler import *
from scheduling.VMPlacementPolicies import *
from Generator import *


parser = argparse.ArgumentParser()
parser.add_argument('--jobs', dest='NO_JOBS', default=100, type=int)
parser.add_argument('--max-threads', dest='MAX_THREADS', default=-1, type=int)
parser.add_argument('--dist-param', dest='TH_BIN_DIST_PARAM', default=0.1, type=float)
parser.add_argument('--seed', dest='SEED', default=-1, type=int)
parser.add_argument('--scheduler', dest='SCHEDULER', default="Simple", type=str,
                    help='options: Simple, BinPacking')
parser.add_argument('--sep-bins', dest='SEP_BINS', action="store_true")
parser.add_argument('--bin-type', dest='BIN_TYPE', default="Simple", type=str,
                    help='options: Simple, Reductive, Timeline, OrderedTimeline')
parser.add_argument('--placement-policy', dest='PLACEMENT_POLICY', default="Simple", type=str,
                    help='options: Simple, Random')
args = parser.parse_args()

if args.SEED >= 0:
    np.random.seed(args.SEED)

if args.MAX_THREADS < 0:
    args.MAX_THREADS = 4

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
VMScheduler = SCHEDULERS[args.SCHEDULER]

PLACEMENT_POLICIES = {
    'Simple': VMPlacementPolicySimple,
    'Random': VMPlacementPolicyRandom,
}
VMPlacementPolicy = PLACEMENT_POLICIES[args.PLACEMENT_POLICY]


no_machines = 0
def getMachine(ram, cores, Scheduler):
    resources = { Resource(Resource.Type.RAM, ram) }
    for core in cores:
        resources.add(Resource(Resource.Type.CPU_core, core))
    global no_machines
    machine = Machine(f"m{no_machines}", resources, lambda m: None, Scheduler)
    no_machines += 1
    return machine


CPU_SPEED = 3.6 # GHz
infrastructure = Infrastructure.getInstance(
    [
        getMachine(32, 8*[CPU_SPEED], VMScheduler),
        getMachine(16, 4*[CPU_SPEED], VMScheduler),
        getMachine(16, 4*[CPU_SPEED], VMScheduler),
        getMachine(8, 4*[CPU_SPEED], VMScheduler),
        getMachine(8, 4*[CPU_SPEED], VMScheduler),
        getMachine(8, 4*[CPU_SPEED], VMScheduler),
        getMachine(8, 4*[CPU_SPEED], VMScheduler),
    ],
    VMPlacementPolicy,
)
max_NO_CORES = 8
total_NO_CORES = 32

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
    noThreads = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                                job.resourceRequest)))
    totalOps += ops
    theoreticalTotalTime += ops / (min(noThreads, max_NO_CORES) * CPU_SPEED)
    vm = CreateVM.minimal([job], ownCores=True)
    vm.scheduleJob(job)
    infrastructure.scheduleVM(vm)

sim = Simulator.getInstance()
sim.simulate()

for machine in infrastructure.machines:
    assert machine._vmScheduler.noVMsLeft == 0

print("simulation time:               ", sim.time)
print("sequence execution time:       ", theoreticalTotalTime)
print("theoretical best possible time:", totalOps / (CPU_SPEED * total_NO_CORES))

