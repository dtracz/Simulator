import nose
from tests.base_test import *
from Simulator import *
from Listeners import EventInspector
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

        infrastructure = Infrastructure(
                [m0, m1],
                VMPlacementPolicySimple,
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


    def test_jobScheduling_withGPU(self):
        inf = INF
        resources = {
            Resource(RType.CPU_core, 10),               # GHz
            Resource(RType.RAM, 16),                    # GB
            Resource(RType.GPU, 1664, 1.05),            # nCC,GHz
        }
        m0 = Machine("m0", resources,
            getJobScheduler=lambda m: JobSchedulerSimple(m, autofree=True))

        job0 = Job(500,
                   [ResourceRequest(RType.CPU_core, inf),
                    ResourceRequest(RType.GPU, 1024),
                    ResourceRequest(RType.RAM, 6)],
               )
        job1 = Job(1000,
                   [ResourceRequest(RType.CPU_core, inf),
                    ResourceRequest(RType.GPU, 1024),
                    ResourceRequest(RType.RAM, 6)],
               )
        job2 = Job(1000,
                   [ResourceRequest(RType.CPU_core, inf),
                    ResourceRequest(RType.GPU, 512),
                    ResourceRequest(RType.RAM, 6)],
               )

        m0.scheduleJob(job0)
        m0.scheduleJob(job1)
        m0.scheduleJob(job2)

        sim = Simulator.getInstance()
        sim.simulate()

        assert sim.time == 250


    def test_vmPlacement_withGPU(self):
        inf = INF
        resources0 = {
            Resource(RType.CPU_core, 10),               # GHz
            Resource(RType.RAM, 16),                    # GB
            Resource(RType.GPU, 1664, 1.05),            # nCC,GHz
        }
        m0 = Machine("m0", resources0, lambda m: None, VMSchedulerSimple)
        resources1 = {
            Resource(RType.CPU_core, 10),               # GHz
            Resource(RType.RAM, 16),                    # GB
            Resource(RType.GPU, 1024, 1.05),            # nCC,GHz
        }
        m1 = Machine("m1", resources1, lambda m: None, VMSchedulerSimple)

        infrastructure = Infrastructure(
                [m0, m1],
                VMPlacementPolicySimple,
        )

        def getVM(vm_id, ram, gpu, req_jobs):
            resourceReq = {
                ResourceRequest(RType.CPU_core, inf, shared=True),
                ResourceRequest(RType.RAM, ram),
                ResourceRequest(RType.GPU, gpu),
            }
            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            jobs = [
                Job(500,
                    [ResourceRequest(RType.CPU_core, inf),
                     ResourceRequest(RType.GPU, 1024),
                     ResourceRequest(RType.RAM, 8)],
                ),
                Job(1000,
                    [ResourceRequest(RType.CPU_core, inf),
                     ResourceRequest(RType.GPU, 1024),
                     ResourceRequest(RType.RAM, 6)],
                ),
                Job(1000,
                    [ResourceRequest(RType.CPU_core, inf),
                     ResourceRequest(RType.GPU, 512),
                     ResourceRequest(RType.RAM, 6)],
                ),
            ]
            for i in req_jobs:
                vm.scheduleJob(jobs[i])
            return vm

        infrastructure.scheduleVM(getVM(0, 8, 1024,[0]))
        infrastructure.scheduleVM(getVM(1, 8, 1024,[1,2]))
        infrastructure.scheduleVM(getVM(2, 16, 1664,[0,1,2]))
        infrastructure.scheduleVM(getVM(3, 12, 1664,[1,2]))

        sim = Simulator.getInstance()
        sim.simulate()
        assert sim.time == 500

    def test_gpuLength_schedule(self):
        inf = INF
        resources = {
            Resource(RType.CPU_core, 10),               # GHz
            Resource(RType.CPU_core, 10),               # GHz
            Resource(RType.RAM, 16),                    # GB
            Resource(RType.GPU, 1664, 1),               # nCC,GHz
        }
        m0 = Machine("m0", resources, JobSchedulerSimple)

        res0 = [
            ResourceRequest(RType.CPU_core, inf),       # GHz
            ResourceRequest(RType.CPU_core, inf),       # GHz
            ResourceRequest(RType.RAM,      5),         # GB
            ResourceRequest(RType.GPU,      1024),      # nCC
        ]
        res1 = [
            ResourceRequest(RType.CPU_core, inf),       # GHz
            ResourceRequest(RType.RAM,      5),         # GB
            ResourceRequest(RType.GPU,      1024),      # nCC
        ]
        job0 = Job({RType.CPU_core: 200, RType.GPU: 1024*15}, res0)
        job1 = Job({RType.CPU_core: 200, RType.GPU: 1024*15}, res1)

        m0.scheduleJob(job0)
        m0.scheduleJob(job1)

        sim = Simulator.getInstance()
        inspector = EventInspector([
            {'time': 0, 'what': NType.JobStart, 'job': job0},
            {'time': 15, 'what': NType.JobFinish, 'job': job0},
            {'time': 15, 'what': NType.JobStart, 'job': job1},
            {'time': 35, 'what': NType.JobFinish, 'job': job1},
        ])
        sim.simulate()
        inspector.verify()

