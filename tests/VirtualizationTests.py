import nose
from tests.base_test import *
from Simulator import *
from Resource import *
from Machine import *
from Job import *


class VirtualizationTests(SimulatorTests):

    def test_allocateVM(self):
        inf = float('inf')
        resources = [
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        ]
        m0 = Machine("m0", resources)
        resourceReq0 = [
            ResourceRequest(Resource.Type.CPU_core, inf),
            ResourceRequest(Resource.Type.CPU_core, inf),
            ResourceRequest(Resource.Type.RAM, 5),
        ]
        vm0 = VirtualMachine("vm0", resourceReq0)
        m0.allocateVM(vm0)
        assert m0._resources[0].avaliableValue == 0
        assert m0._resources[1].avaliableValue == 0
        assert m0._resources[2].avaliableValue == 11
        assert vm0._resources[0].maxValue == 10
        assert vm0._resources[1].maxValue == 10
        assert vm0._resources[2].maxValue == 5

    def test_freeVM(self):
        inf = float('inf')
        resources = [
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        ]
        m0 = Machine("m0", resources)
        resourceReq0 = [
            ResourceRequest(Resource.Type.CPU_core, inf),
            ResourceRequest(Resource.Type.CPU_core, inf),
            ResourceRequest(Resource.Type.RAM, 5),
        ]
        vm0 = VirtualMachine("vm0", resourceReq0)
        m0.allocateVM(vm0)
        m0.freeVM(vm0)
        assert m0._resources[0].avaliableValue == 10
        assert isinstance(m0._resources[0], SharedResource)
        assert m0._resources[1].avaliableValue == 10
        assert isinstance(m0._resources[1], SharedResource)
        assert m0._resources[2].avaliableValue == 16
        assert 0 == len(vm0._resources)


    def test_2jobsOn2VMs(self):
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)
        resourceReq0 = {
            ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
            ResourceRequest(Resource.Type.RAM, 8),
        }
        vm0 = VirtualMachine("vm0", resourceReq0)
        resourceReq1 = {
            ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
            ResourceRequest(Resource.Type.RAM, 8),
        }
        vm1 = VirtualMachine("vm1", resourceReq0)
        m0.allocateVM(vm0)
        m0.allocateVM(vm1)

        job0 = Job(100,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 5)],
                   vm0
               )
        job1 = Job(100,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 5)],
                   vm1
               )

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))
        inspector = EventInspector([
            (0, "JobStart_Job_0"),
            (0, "JobStart_Job_1"),
            (20, "JobFinish_Job_0"),
            (20, "JobFinish_Job_1"),
        ])
        sim.simulate()
        inspector.verify()

        m0.freeVM(vm0)
        m0.freeVM(vm1)

