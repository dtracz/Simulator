from Simulator import *
from Resource import *
from Machine import *
from Schedulers import *
from BinPackingScheduler import *
from Generator import *
from numpy import random

CPU_SPEED = 3.6
RAM_SIZE = 16

random.seed(1)

resources = {
    Resource(Resource.Type.CPU_core, CPU_SPEED), # GHz
    Resource(Resource.Type.CPU_core, CPU_SPEED), # GHz
    Resource(Resource.Type.CPU_core, CPU_SPEED), # GHz
    Resource(Resource.Type.CPU_core, CPU_SPEED), # GHz
    Resource(Resource.Type.RAM, RAM_SIZE),       # GB
}
machine = Machine("m0", resources, lambda m: None, VMSchedulerSimple)
#  machine = Machine("m0", resources, lambda m: None, BinPackingScheduler)


gen = RandomJobGenerator(noCores=lambda s: 1+random.binomial(3, 0.08, s))
jobs = gen.getJobs(100)

totalOps = 0
theoreticalTotalTime = 0
for job in jobs:
    ops = job.operations
    noCores = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                              job.resourceRequest)))
    totalOps += ops
    theoreticalTotalTime += ops / (noCores*CPU_SPEED)
    vm = CreateVM.minimal([job])
    vm.scheduleJob(job)
    machine.scheduleVM(vm)

sim = Simulator.getInstance()
sim.simulate()

print("simulation time:", sim.time)
print("theoretical sequence execution time:", theoreticalTotalTime)
print("theoretical best possible time:", totalOps / (CPU_SPEED*4))

