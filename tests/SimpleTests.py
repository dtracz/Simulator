import nose
from tests.base_test import *
from toolkit import INF
from Simulator import *
from Resource import *
from Machine import *
from Job import *


class SimpleTests(SimulatorTests):
 
    def test_1job(self):
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),    # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(Resource.Type.CPU_core, 10), # GHz
            ResourceRequest(Resource.Type.RAM,      5),  # GB
        ]
        job0 = Job(100, res0, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))

        sim.simulate()
        assert sim.time == 10


    def test_2jobs2cores(self):
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(Resource.Type.CPU_core, 10), # GHz
            ResourceRequest(Resource.Type.RAM,      5),  # GB
        ]
        res1 = [
            ResourceRequest(Resource.Type.CPU_core, 10), # GHz
            ResourceRequest(Resource.Type.RAM,      8),  # GB
        ]
        job0 = Job(650, res0, m0)
        job1 = Job(450, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        sim.simulate()
        assert sim.time == 65

    def test_1job2cores(self):
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.CPU_core, 5),  # GHz
            Resource(Resource.Type.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(Resource.Type.CPU_core, 10), # GHz
            ResourceRequest(Resource.Type.CPU_core, 5),  # GHz
            ResourceRequest(Resource.Type.RAM,      5),  # GB
        ]
        job0 = Job(150, res0, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))

        sim.simulate()
        assert sim.time == 10


    def test_ramFailure(self):
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(Resource.Type.CPU_core, 10), # GHz
            ResourceRequest(Resource.Type.RAM,      10),  # GB
        ]
        res1 = [
            ResourceRequest(Resource.Type.CPU_core, 10), # GHz
            ResourceRequest(Resource.Type.RAM,      8),  # GB
        ]
        job0 = Job(650, res0, m0)
        job1 = Job(450, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        cought = False
        try:
            sim.simulate()
        except RuntimeError as e:
            cought = e.args[0] == "Requested 8 out of 6 avaliable" or \
                     e.args[0] == "Cannot find fitting Type.RAM" or \
                     e.args[0] == "Resources allocation for Job_1 failed"
        assert cought


    def test_3coresJobOn1coresMachine(self):
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)
        req0 = [
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        job0 = Job(75, req0, m0)
        req1 = [
            ResourceRequest(Resource.Type.CPU_core, INF), # GHz
            ResourceRequest(Resource.Type.RAM,      5),   # GB
        ]
        job1 = Job(25+20, req1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        inspector = EventInspector([
            (0, "JobStart_Job_0"),
            (0, "JobStart_Job_1"),
            (10, "JobFinish_Job_0"),
            (12, "JobFinish_Job_1"),
        ])
        sim.simulate()
        inspector.verify()

