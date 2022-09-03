import nose
from tests.base_test import *
from toolkit import INF
from Simulator import *
from Listeners import EventInspector
from Resource import *
from Machine import *
from Job import *


class SimpleTests(SimulatorTests):
 
    def test_1job(self):
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),    # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(RType.CPU_core, 10), # GHz
            ResourceRequest(RType.RAM,      5),  # GB
        ]
        job0 = Job(100, res0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))

        sim.simulate()
        assert sim.time == 10


    def test_2jobs2cores(self):
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(RType.CPU_core, 10), # GHz
            ResourceRequest(RType.RAM,      5),  # GB
        ]
        res1 = [
            ResourceRequest(RType.CPU_core, 10), # GHz
            ResourceRequest(RType.RAM,      8),  # GB
        ]
        job0 = Job(650, res0)
        job1 = Job(450, res1)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))
        sim.addEvent(0, JobStart(job1, m0))

        sim.simulate()
        assert sim.time == 65

    def test_1job2cores(self):
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.CPU_core, 5),  # GHz
            Resource(RType.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(RType.CPU_core, 10), # GHz
            ResourceRequest(RType.CPU_core, 5),  # GHz
            ResourceRequest(RType.RAM,      5),  # GB
        ]
        job0 = Job(150, res0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))

        sim.simulate()
        assert sim.time == 10


    def test_ramFailure(self):
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(RType.CPU_core, 10), # GHz
            ResourceRequest(RType.RAM,      10),  # GB
        ]
        res1 = [
            ResourceRequest(RType.CPU_core, 10), # GHz
            ResourceRequest(RType.RAM,      8),  # GB
        ]
        job0 = Job(650, res0)
        job1 = Job(450, res1)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))
        sim.addEvent(0, JobStart(job1, m0))

        cought = False
        try:
            sim.simulate()
        except RuntimeError as e:
            cought = e.args[0] == "Requested 8 out of 6 avaliable" or \
                     e.args[0] == "Cannot find fitting Type.RAM" or \
                     e.args[0] == "Resources allocation for Job_1 on m0 failed"
        assert cought


    def test_3coresJobOn1coresMachine(self):
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),      # GB
        }
        m0 = Machine("m0", resources)
        req0 = [
            ResourceRequest(RType.CPU_core, INF), # GHz
            ResourceRequest(RType.CPU_core, INF), # GHz
            ResourceRequest(RType.CPU_core, INF), # GHz
            ResourceRequest(RType.RAM,      5),   # GB
        ]
        job0 = Job(75, req0)
        req1 = [
            ResourceRequest(RType.CPU_core, INF), # GHz
            ResourceRequest(RType.RAM,      5),   # GB
        ]
        job1 = Job(25+20, req1)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))
        sim.addEvent(0, JobStart(job1, m0))

        inspector = EventInspector([
            {'time': 0, 'what': NType.JobStart, 'job': job0},
            {'time': 0, 'what': NType.JobStart, 'job': job1},
            {'time': 10, 'what': NType.JobFinish, 'job': job0},
            {'time': 12, 'what': NType.JobFinish, 'job': job1},
        ])
        sim.simulate()
        inspector.verify()

