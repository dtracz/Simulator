import nose
from tests.base_test import *
from Simulator import *
from Resource import *
from Machine import *
from Job import *


class VirtualizationTests(SimulatorTests):

    def test_allocateVM(self):
        inf = INF
        resources = [
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        ]
        m0 = Machine("m0", resources)
        resourceReq0 = [
            ResourceRequest(Resource.Type.CPU_core, inf, shared=False),
            ResourceRequest(Resource.Type.CPU_core, inf, shared=False),
            ResourceRequest(Resource.Type.RAM, 5, shared=False),
        ]
        vm0 = VirtualMachine("vm0", resourceReq0)
        m0.allocate(vm0)
        assert m0.resources[0].avaliableValue == 0
        assert m0.resources[1].avaliableValue == 0
        assert m0.resources[2].avaliableValue == 11
        assert vm0.resources[0].maxValue == 10
        assert vm0.resources[1].maxValue == 10
        assert vm0.resources[2].maxValue == 5

    def test_freeVM(self):
        inf = INF
        resources = [
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        ]
        m0 = Machine("m0", resources)
        resourceReq0 = [
            ResourceRequest(Resource.Type.CPU_core, inf, shared=False),
            ResourceRequest(Resource.Type.CPU_core, inf, shared=False),
            ResourceRequest(Resource.Type.RAM, 5, shared=False),
        ]
        vm0 = VirtualMachine("vm0", resourceReq0)
        m0.allocate(vm0)
        m0.free(vm0)
        assert m0.resources[0].avaliableValue == 10
        assert isinstance(m0.resources[0], Resource)
        assert m0.resources[1].avaliableValue == 10
        assert isinstance(m0.resources[1], Resource)
        assert m0.resources[2].avaliableValue == 16
        assert 0 == len(vm0.resources)


    def test_2jobsOn2VMs(self):
        inf = INF
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)
        resourceReq0 = {
            ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
            ResourceRequest(Resource.Type.RAM, 8, shared=False),
        }
        vm0 = VirtualMachine("vm0", resourceReq0)
        resourceReq1 = {
            ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
            ResourceRequest(Resource.Type.RAM, 8, shared=False),
        }
        vm1 = VirtualMachine("vm1", resourceReq0)
        m0.allocate(vm0)
        m0.allocate(vm1)

        job0 = Job(100,
                   [ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
                    ResourceRequest(Resource.Type.RAM, 5, shared=False)],
               )
        job1 = Job(100,
                   [ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
                    ResourceRequest(Resource.Type.RAM, 5, shared=False)],
               )

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, vm0))
        sim.addEvent(0, JobStart(job1, vm1))
        inspector = EventInspector([
            {'time': 0, 'what': NType.JobStart, 'job': job0},
            {'time': 0, 'what': NType.JobStart, 'job': job1},
            {'time': 20, 'what': NType.JobFinish, 'job': job0},
            {'time': 20, 'what': NType.JobFinish, 'job': job1},
        ])
        sim.simulate()
        inspector.verify()

        m0.free(vm0)
        m0.free(vm1)


    def test_2coresVmOn1coresMachine(self):
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)
        req = [
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.RAM,      INF), # GB
        ]
        vm0 = VirtualMachine("vm0", req)
        req0 = [
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        job0 = Job(75, req0)
        req1 = [
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        job1 = Job(25+20, req1)

        sim = Simulator.getInstance()
        sim.addEvent(0, VMStart(m0, vm0))
        sim.addEvent(0, JobStart(job0, vm0))
        sim.addEvent(0, JobStart(job1, vm0))

        inspector = EventInspector([
            {'time': 0, 'what': NType.VMStart, 'vm': vm0},
            {'time': 0, 'what': NType.JobStart, 'job': job0},
            {'time': 0, 'what': NType.JobStart, 'job': job1},
            {'time': 10, 'what': NType.JobFinish, 'job': job0},
            {'time': 12, 'what': NType.JobFinish, 'job': job1},
        ])
        sim.simulate()
        inspector.verify()

