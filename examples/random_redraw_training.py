import sys
import argparse
import numpy as np
from Simulator import *
from Listeners import *
from Resource import *
from Machine import *
from scheduling.BaseSchedulers import *
from scheduling.BinPackingScheduler import *
from scheduling.VMPlacementPolicies import *
from scheduling.Models import *
from scheduling.Trainers import *
from Generator import *
from toolkit import Global



#---FUNCIONS--------------------------------------------------------------------

def getJobTime(job):
    global best_CPU_set
    no_cores, cpu_freq = best_CPU_set
    noCPU_threads = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                                    job.resourceRequest)))
    cpu_cores = min(noCPU_threads, no_cores)
    cpuTime = job.operations.get(RType.CPU_core, 0) / (cpu_cores * cpu_freq)

    global best_GPU_set
    no_gpus, gpu_pow = best_GPU_set
    noGPUs = len(list(filter(lambda r: r.rtype == Resource.Type.GPU,
                                    job.resourceRequest)))
    gpu_cores = min(noGPUs, no_gpus)
    gpuTime = job.operations.get(RType.GPU, 0) / (gpu_cores * gpu_pow + 1e-8)
    return max(cpuTime, gpuTime)


def run_simulation(generator, infrastructure, model, n_repeats=5):
    if model is not None:
        policy = infrastructure._vmPlacementPolicy
        assert isinstance(policy, VMPlacementPolicyAI)
        policy._model = model

    score = 0
    for _ in range(n_repeats):
        sim = Simulator.getInstance()
        sim._eventQueue._currentTime = 0
        sim._eventQueue._done = []

        jobs = generator.getJobs(args.NO_JOBS)
        #  totalOps = {}
        #  seqTime = 0
        vms = []
        for job in jobs:
            #  seqTime += getJobTime(job)
            #  totalOps = dictPlus(totalOps, job.operations)
            vm = CreateVM.minimal([job], ownCores=True)
            vm.scheduleJob(job)
            #  infrastructure.scheduleVM(vm)
            vms += [vm]

        delayScheduler = VMDelayScheduler(infrastructure,
                lambda n: np.random.uniform(0, args.SPAN, n))
        delayScheduler.scheduleVM(vms)

        metric = AUPMetric()
        sim.simulate()
        metric.unregister()

        for machine in infrastructure.machines:
            assert machine._vmScheduler.noVMsLeft == 0
        #  print("t:", sim.time)
        #  score += sim.time
        score += metric.cost(args.SPAN == 0)
    return score / n_repeats


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
parser.add_argument('--inf', dest='INF', default="./infrastructure2.json", type=str,
                    help='path to file with infrastructure decription')
parser.add_argument('--save-vars', dest='VARFILE', default=None, type=str,
                    help='path to create files to save model weights')
parser.add_argument('--epochs', dest='N_EPOCHS', default=100, type=int)
args = parser.parse_args()

if args.SEED >= 0:
    np.random.seed(args.SEED)
if args.MAX_THREADS < 0:
    args.MAX_THREADS = 8
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
VMScheduler = SCHEDULERS[args.SCHEDULER]
Model = Model_v0_np
VMPlacementPolicy = lambda *largs, **kwargs: \
        VMPlacementPolicyAI(*largs, **kwargs, ModelClass=Model) 


#---GET-INFRASTRUCTURE----------------------------------------------------------

no_machines = 0
def getMachine(ram, cores, gpus, Scheduler):
    resources = { Resource(Resource.Type.RAM, ram) }
    for core in cores:
        resources.add(Resource(Resource.Type.CPU_core, core))
    for n_CC, freq in gpus:
        resources.add(Resource(Resource.Type.GPU, n_CC, freq=freq))
    global no_machines
    machine = Machine(f"m{no_machines}", resources, lambda m: None, Scheduler)
    no_machines += 1
    return machine


Global.load(args.INF)
machines = []
best_CPU_set = (0,0)
total_CPU_power = 0
best_GPU_set = (0,0)
total_GPU_power = 0
for MACHINE in Global.MACHINES:
    machine = getMachine(MACHINE.RAM,
                         MACHINE.CPU_CORES,
                         MACHINE.GPUS,
                         VMScheduler,
    )
    machines += [machine]
    if np.sum(MACHINE.CPU_CORES) > np.prod(best_CPU_set):
        best_CPU_set = (len(MACHINE.CPU_CORES), np.mean(MACHINE.CPU_CORES))
    total_CPU_power += np.sum(MACHINE.CPU_CORES)
    if MACHINE.GPUS == []:
        continue
    if np.prod(MACHINE.GPUS, axis=1).sum() > np.prod(best_GPU_set):
        best_GPU_set = (len(MACHINE.GPUS), np.prod(MACHINE.GPUS, axis=1).mean())
    total_GPU_power += np.prod(MACHINE.GPUS, axis=1).sum()
infrastructure = Infrastructure(machines, VMPlacementPolicy)


#---GET-JOBS--------------------------------------------------------------------

def priorities(s):
    ''' return list of `s` random functions `int -> int` '''
    a_s = np.abs(np.random.normal(1, 0.3*args.PP, s))/100
    b_s = np.abs(np.random.normal(1, 1.0*args.PP, s))
    return [(lambda t, a=a, b=b: a*t + b) for a, b in zip(a_s, b_s)]

gen = RandomJobGenerator(
    noCores=lambda s: 1 + np.random.binomial(
            args.MAX_THREADS-1,
            args.TH_BIN_DIST_PARAM,
            s,
    ),
    #  noGPUs=lambda s: np.zeros(s, dtype=np.int32),
    priorities=priorities,
)
jobs = gen.getJobs(args.NO_JOBS)


#---TRAINING--------------------------------------------------------------------

model = infrastructure._vmPlacementPolicy._model
score_fun = lambda m: -run_simulation(gen, infrastructure, m)
trainer = RandomTrainer(model, score_fun, epoch_size=20, n_bests=4)
for i in range(args.N_EPOCHS):
    if args.VARFILE:
        model = infrastructure._vmPlacementPolicy._model
        model.saveVars(f"{args.VARFILE}_{i}")
    score = trainer.step()
    print(i, score, sep=':\t')
    sys.stdout.flush()
if args.VARFILE:
    model = infrastructure._vmPlacementPolicy._model
    model.saveVars(f"{args.VARFILE}_{args.N_EPOCHS}")

