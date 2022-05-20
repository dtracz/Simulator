from Simulator import *
from Resource import *
from Machine import *
from Schedulers import *
from Generator import *
from numpy import random

CPU_SPEED = 3.6

random.seed(1)

resources = {
    SharedResource(Resource.Type.CPU_core, CPU_SPEED), # GHz
    SharedResource(Resource.Type.CPU_core, CPU_SPEED), # GHz
    SharedResource(Resource.Type.CPU_core, CPU_SPEED), # GHz
    SharedResource(Resource.Type.CPU_core, CPU_SPEED), # GHz
    Resource(Resource.Type.RAM, 16),            # GB
}
machine = Machine("m0", resources, lambda m: None, VMSchedulerSimple)


gen = RandomJobGenerator(noCores=lambda s: 1+random.binomial(3, 0.08, s))
jobs = gen.getJobs(1000)

totalOps = 0
theoreticalTotalTime = 0
for job in jobs:
    ops = job.operations
    noCores = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                              job.resourceRequest)))
    totalOps += ops
    theoreticalTotalTime += ops / (noCores*CPU_SPEED)
    vm = CreateVM.minimal([job])
    job.asignMachine(vm)
    vm.scheduleJob(job)
    machine.scheduleVM(vm)

sim = Simulator.getInstance()
sim.simulate()

print("simulation time:", sim.time)
print("teoretical sequence execution time:", theoreticalTotalTime)
print("teoretical best possible time:", totalOps / (CPU_SPEED*4))

