import nose
from tests.base_test import *
from Simulator import *
from Resource import *
from Machine import *
from Job import *
from Schedulers import *


class SchedulersTests(SimulatorTests):

    def test_jobSchedulerSimple(self):
        inf = INF
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        resourceReq0 = {
            ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
            ResourceRequest(Resource.Type.RAM, inf),
        }
        vm0 = VirtualMachine("vm0", resourceReq0,
                lambda machine: JobSchedulerSimple(machine, autofree=True))

        job0 = Job(500,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 8)],
                   vm0
               )
        job1 = Job(1000,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 6)],
                   vm0
               )
        job2 = Job(1000,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 6)],
                   vm0
               )

        vm0.scheduleJob(job0)
        vm0.scheduleJob(job1)
        vm0.scheduleJob(job2)

        sim = Simulator.getInstance()
        sim.addEvent(0, VMStart(m0, vm0))
        sim.simulate()

        assert sim.time == 250


    def test_vmSchedulerSimple(self):
        inf = INF
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources, lambda m: None, VMSchedulerSimple)

        def getVM(vm_id):
            resourceReq = {
                ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
                ResourceRequest(Resource.Type.RAM, 8),
            }

            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            job0 = Job(500,
                       [ResourceRequest(Resource.Type.CPU_core, inf),
                        ResourceRequest(Resource.Type.RAM, 8)],
                       vm
                   )
            job1 = Job(1000,
                       [ResourceRequest(Resource.Type.CPU_core, inf),
                        ResourceRequest(Resource.Type.RAM, 6)],
                       vm
                   )
            job2 = Job(1000,
                       [ResourceRequest(Resource.Type.CPU_core, inf),
                        ResourceRequest(Resource.Type.RAM, 6)],
                       vm
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
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 8),            # GB
        }
        m0 = Machine("m0", resources, lambda m: None, VMSchedulerSimple)
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m1 = Machine("m1", resources, lambda m: None, VMSchedulerSimple)

        infrastructure = Infrastructure.getInstance(
                [m0, m1],
                VMPlacmentPolicySimple,
        )

        def getVM(vm_id, ram, req_jobs):
            resourceReq = {
                ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
                ResourceRequest(Resource.Type.RAM, ram),
            }
            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            jobs = [
                Job(500,
                    [ResourceRequest(Resource.Type.CPU_core, inf),
                     ResourceRequest(Resource.Type.RAM, 8)],
                    vm
                ),
                Job(1000,
                    [ResourceRequest(Resource.Type.CPU_core, inf),
                     ResourceRequest(Resource.Type.RAM, 6)],
                    vm
                ),
                Job(1000,
                    [ResourceRequest(Resource.Type.CPU_core, inf),
                     ResourceRequest(Resource.Type.RAM, 6)],
                    vm
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

