import argparse
import numpy as np
from Simulator import *
from Resource import *
from Machine import *
from scheduling.BaseSchedulers import *
from scheduling.BinPackingScheduler import *
from scheduling.VMPlacementPolicies import *
from scheduling.Models import *
from scheduling.Trainers import *
from Generator import *

CPU_SPEED = 3.6 # GHz
MAX_THREADS = 8
TH_BIN_DIST_PARAM = 0.1
SEP_BINS = False
NO_JOBS = 100


no_machines = 0
def getMachine(ram, cores, Scheduler):
    resources = { Resource(Resource.Type.RAM, ram) }
    for core in cores:
        resources.add(Resource(Resource.Type.CPU_core, core))
    global no_machines
    machine = Machine(f"m{no_machines}", resources, lambda m: None, Scheduler)
    no_machines += 1
    return machine

def run_simulation(generator, infrastructure, model, n_repeats=5):
    if model is not None:
        policy = infrastructure._vmPlacementPolicy
        assert type(policy) is VMPlacementPolicyAI
        policy._model = model

    score = 0
    for _ in range(n_repeats):
        sim = Simulator.getInstance()
        sim._eventQueue._currentTime = 0

        jobs = generator.getJobs(NO_JOBS)
        totalOps = 0
        theoreticalTotalTime = 0
        for job in jobs:
            ops = job.operations[RType.CPU_core]
            noThreads = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                                        job.resourceRequest)))
            totalOps += ops
            theoreticalTotalTime += ops / (min(noThreads, max_NO_CORES) * CPU_SPEED)
            vm = CreateVM.minimal([job], ownCores=True)
            vm.scheduleJob(job)
            infrastructure.scheduleVM(vm)
        sim.simulate()

        for machine in infrastructure.machines:
            assert machine._vmScheduler.noVMsLeft == 0
        score += sim.time
    return score



VMScheduler = lambda *largs, **kwargs: \
    BinPackingScheduler(*largs, **kwargs, BinClass=SimpleBin, awaitBins=SEP_BINS)
VMPlacementPolicy = lambda *largs, **kwargs: \
    VMPlacementPolicyAI(*largs, **kwargs, ModelClass=Model_v0_np)
infrastructure = Infrastructure(
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

generator = RandomJobGenerator(
    noCores=lambda s: 1 + np.random.binomial(
            MAX_THREADS-1,
            TH_BIN_DIST_PARAM,
            s,
    ),
    noGPUs=lambda s: np.zeros(s, dtype=np.int32),
)

model = infrastructure._vmPlacementPolicy._model
score_fun = lambda m: -run_simulation(generator, infrastructure, m)
trainer = RandomTrainer(model, score_fun, epoch_size=20, n_bests=4)
for _ in range(100):
    score = trainer.step()
    print(score)

