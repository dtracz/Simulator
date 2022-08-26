import argparse
import numpy as np
from Simulator import *
from Resource import *
from Machine import *
from scheduling.BaseSchedulers import *
from scheduling.BinPackingScheduler import *
from scheduling.VMPlacementPolicies import *
from scheduling.Models import *
from Generator import *
from toolkit import Global



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
                    help='options: Simple, Random, AI')
parser.add_argument('--model', dest='MODEL', default="Random", type=str,
                    help='options: Random')
parser.add_argument('--inf', dest='INF', default="./infrastructure2.json", type=str,
                    help='path to file with infrastructure decription')
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

MODELS = {
    'Random': RandomModel,
}
Model = MODELS[args.MODEL]
PLACEMENT_POLICIES = {
    'Simple': VMPlacementPolicySimple,
    'Random': VMPlacementPolicyRandom,
    'AI': lambda *largs, **kwargs: \
        VMPlacementPolicyAI(*largs, **kwargs, ModelClass=Model),
}
VMPlacementPolicy = PLACEMENT_POLICIES[args.PLACEMENT_POLICY]


no_machines = 0
def getMachine(ram, cores, gpus, Scheduler):
    resources = { Resource(Resource.Type.RAM, ram) }
    for core in cores:
        resources.add(Resource(Resource.Type.CPU_core, core))
    global no_machines
    machine = Machine(f"m{no_machines}", resources, lambda m: None, Scheduler)
    no_machines += 1
    return machine


Global.load(args.INF)
machines = []
max_NO_CORES = 0
total_NO_CORES = 0
for MACHINE in Global.MACHINES:
    machine = getMachine(MACHINE.RAM,
                         MACHINE.CPU_CORES*[Global.CPU_SPEED],
                         MACHINE.GPUS*[(Global.N_CC, Global.GPU_SPEED)],
                         VMScheduler,
    )
    machines += [machine]
    max_NO_CORES = max(max_NO_CORES, MACHINE.CPU_CORES)
    total_NO_CORES += MACHINE.CPU_CORES
infrastructure = Infrastructure(machines, VMPlacementPolicy)

gen = RandomJobGenerator(
    noCores=lambda s: 1 + np.random.binomial(
            args.MAX_THREADS-1,
            args.TH_BIN_DIST_PARAM,
            s,
    ),
    noGPUs=lambda s: np.zeros(s, dtype=np.int32),
)
jobs = gen.getJobs(args.NO_JOBS)

def getJobTime(job):
    ops = job.operations
    noCPU_threads = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                                    job.resourceRequest)))
    cpu_cores = min(noCPU_threads, max_NO_CORES)
    cpuTime = job.operations.get(RType.CPU_core, 0) / (cpu_cores * Global.CPU_SPEED + 1e-8)
    #  noGPUs = len(list(filter(lambda r: r.rtype == Resource.Type.GPU,
    #                                  job.resourceRequest)))
    #  gpu_cores = min(noGPUs, args.NO_GPUS) * args.N_CC
    #  gpuTime = job.operations.get(RType.GPU, 0) / (gpu_cores * args.GPU_SPEED + 1e-8)
    #  return max(cpuTime, gpuTime)
    return cpuTime


seqTime = 0
totalOps = {}
for job in jobs:
    seqTime += getJobTime(job)
    totalOps = dictPlus(totalOps, job.operations)
    vm = CreateVM.minimal([job], ownCores=True)
    vm.scheduleJob(job)
    infrastructure.scheduleVM(vm)

sim = Simulator.getInstance()
sim.simulate()

for machine in infrastructure.machines:
    assert machine._vmScheduler.noVMsLeft == 0

thCPUBestTime = totalOps.get(RType.CPU_core, 0) / (total_NO_CORES * Global.CPU_SPEED)
print("simulation time:               ", sim.time)
print("sequence execution time:       ", seqTime)
print("theoretical best possible time:", thCPUBestTime)

