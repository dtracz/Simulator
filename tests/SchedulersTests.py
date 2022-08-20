import nose
from tests.base_test import *
from Simulator import *
from Resource import *
from Machine import *
from Job import *
from scheduling.BaseSchedulers import *


class SchedulersTests(SimulatorTests):

    def test_jobSchedulerSimple(self):
        inf = INF
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources,
            getJobScheduler=lambda m: JobSchedulerSimple(m, autofree=True))

        job0 = Job(500,
                   [ResourceRequest(RType.CPU_core, inf),
                    ResourceRequest(RType.RAM, 8)],
               )
        job1 = Job(1000,
                   [ResourceRequest(RType.CPU_core, inf),
                    ResourceRequest(RType.RAM, 6)],
               )
        job2 = Job(1000,
                   [ResourceRequest(RType.CPU_core, inf),
                    ResourceRequest(RType.RAM, 6)],
               )

        m0.scheduleJob(job0)
        m0.scheduleJob(job1)
        m0.scheduleJob(job2)

        sim = Simulator.getInstance()
        sim.simulate()

        assert sim.time == 250


    def test_vmSchedulerSimple(self):
        inf = INF
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources, lambda m: None, VMSchedulerSimple)

        def getVM(vm_id):
            resourceReq = {
                ResourceRequest(RType.CPU_core, inf, shared=True),
                ResourceRequest(RType.RAM, 8),
            }

            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            job0 = Job(500,
                       [ResourceRequest(RType.CPU_core, inf),
                        ResourceRequest(RType.RAM, 8)],
                   )
            job1 = Job(1000,
                       [ResourceRequest(RType.CPU_core, inf),
                        ResourceRequest(RType.RAM, 6)],
                   )
            job2 = Job(1000,
                       [ResourceRequest(RType.CPU_core, inf),
                        ResourceRequest(RType.RAM, 6)],
                   )
            vm.scheduleJob(job0)
            vm.scheduleJob(job1)
            vm.scheduleJob(job2)
            return vm

        m0.scheduleVM(getVM(0))
        m0.scheduleVM(getVM(1))
        m0.scheduleVM(getVM(2))
        m0.scheduleVM(getVM(3))

        sim = Simulator.getInstance()
        sim.simulate()

        assert sim.time == 1000


    def test_placementPolicySimple(self):
        inf = INF
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 8),            # GB
        }
        m0 = Machine("m0", resources, lambda m: None, VMSchedulerSimple)
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),            # GB
        }
        m1 = Machine("m1", resources, lambda m: None, VMSchedulerSimple)

        infrastructure = Infrastructure.getInstance(
                [m0, m1],
                VMPlacmentPolicySimple,
        )

        def getVM(vm_id, ram, req_jobs):
            resourceReq = {
                ResourceRequest(RType.CPU_core, inf, shared=True),
                ResourceRequest(RType.RAM, ram),
            }
            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            jobs = [
                Job(500,
                    [ResourceRequest(RType.CPU_core, inf),
                     ResourceRequest(RType.RAM, 8)],
                ),
                Job(1000,
                    [ResourceRequest(RType.CPU_core, inf),
                     ResourceRequest(RType.RAM, 6)],
                ),
                Job(1000,
                    [ResourceRequest(RType.CPU_core, inf),
                     ResourceRequest(RType.RAM, 6)],
                ),
            ]
            for i in req_jobs:
                vm.scheduleJob(jobs[i])
            return vm

        infrastructure.scheduleVM(getVM(0, 8, [0]))
        infrastructure.scheduleVM(getVM(0, 8, [1,2]))
        infrastructure.scheduleVM(getVM(0, 16, [0,1,2]))
        infrastructure.scheduleVM(getVM(0, 12, [1,2]))

        sim = Simulator.getInstance()
        sim.simulate()
        assert sim.time == 650

