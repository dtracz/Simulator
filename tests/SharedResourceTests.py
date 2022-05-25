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
        job0 = Job(200, res0, m0)
        job1 = Job(200, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

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
        job0 = Job(600, res0, m0)
        job1 = Job(400, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(30, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

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
        job0 = Job(20, res0, m0)
        job1 = Job(40, res1, m0)
        job2 = Job(50, res2, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(5, JobStart(job1))
        sim.addEvent(5, JobStart(job2))

        inspector = EventInspector([
            (0, "JobStart_Job_0"),
            (5, "JobStart_Job_1"),
            (5, "JobStart_Job_2"),
            (10, "JobFinish_Job_0"),
            (14, "JobFinish_Job_1"),
            (15, "JobFinish_Job_2"),
        ])
        sim.simulate()
        inspector.verify()

