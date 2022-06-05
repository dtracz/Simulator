import nose
from tests.base_test import *
from Simulator import *
from Resource import *
from Machine import *
from Job import *


class ResourceTests(SimulatorTests):

    def test_1coreSimple(self):
        inf = INF
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(Resource.Type.CPU_core, inf), # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        res1 = [
            ResourceRequest(Resource.Type.CPU_core, inf), # GHz
            ResourceRequest(Resource.Type.RAM,      8),   # GB
        ]
        job0 = Job(200, res0)
        job1 = Job(200, res1)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))
        sim.addEvent(0, JobStart(job1, m0))

        sim.simulate()
        assert sim.time == 40


    def test_1vs2(self):
        inf = INF
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(Resource.Type.CPU_core, inf), # GHz
            ResourceRequest(Resource.Type.CPU_core, inf), # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        res1 = [
            ResourceRequest(Resource.Type.CPU_core, inf), # GHz
            ResourceRequest(Resource.Type.RAM,      8),   # GB
        ]
        job0 = Job(600, res0)
        job1 = Job(400, res1)

        sim = Simulator.getInstance()
        sim.addEvent(30, JobStart(job0, m0))
        sim.addEvent(0, JobStart(job1, m0))

        sim.simulate()
        assert sim.time == 65


    def test_witholdFromShared(self):
        inf = INF
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(Resource.Type.CPU_core, 2),   # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        res1 = [
            ResourceRequest(Resource.Type.CPU_core, inf), # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        res2 = [
            ResourceRequest(Resource.Type.CPU_core, inf), # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        job0 = Job(20, res0)
        job1 = Job(40, res1)
        job2 = Job(50, res2)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))
        sim.addEvent(5, JobStart(job1, m0))
        sim.addEvent(5, JobStart(job2, m0))

        inspector = EventInspector([
            {'time': 0, 'what': NType.JobStart, 'job': job0},
            {'time': 5, 'what': NType.JobStart, 'job': job1},
            {'time': 5, 'what': NType.JobStart, 'job': job2},
            {'time': 10, 'what': NType.JobFinish, 'job': job0},
            {'time': 14, 'what': NType.JobFinish, 'job': job1},
            {'time': 15, 'what': NType.JobFinish, 'job': job2},
        ])
        sim.simulate()
        inspector.verify()

