import nose
from tests.base_test import *
from Simulator import *
from Listeners import EventInspector
from Resource import *
from Machine import *
from Job import *


class ResourceTests(SimulatorTests):

    def test_1coreSimple(self):
        inf = INF
        resources = {
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(RType.CPU_core, inf), # GHz
            ResourceRequest(RType.RAM,      5),   # GB
        ]
        res1 = [
            ResourceRequest(RType.CPU_core, inf), # GHz
            ResourceRequest(RType.RAM,      8),   # GB
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
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(RType.CPU_core, inf), # GHz
            ResourceRequest(RType.CPU_core, inf), # GHz
            ResourceRequest(RType.RAM,      5),   # GB
        ]
        res1 = [
            ResourceRequest(RType.CPU_core, inf), # GHz
            ResourceRequest(RType.RAM,      8),   # GB
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
            Resource(RType.CPU_core, 10), # GHz
            Resource(RType.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(RType.CPU_core, 2),   # GHz
            ResourceRequest(RType.RAM,      5),   # GB
        ]
        res1 = [
            ResourceRequest(RType.CPU_core, inf), # GHz
            ResourceRequest(RType.RAM,      5),   # GB
        ]
        res2 = [
            ResourceRequest(RType.CPU_core, inf), # GHz
            ResourceRequest(RType.RAM,      5),   # GB
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


    def test_3resources(self):
        inf = INF
        resources = {
            Resource(RType.CPU_core, 10),               # GHz
            Resource(RType.RAM, 16),                    # GB
            Resource(RType.GPU, 1664, 1.05),            # nCC,GHz
        }
        m0 = Machine("m0", resources)

        res0 = [
            ResourceRequest(RType.CPU_core, inf),       # GHz
            ResourceRequest(RType.RAM,      5),         # GB
            ResourceRequest(RType.GPU,      512),       # nCC
        ]
        res1 = [
            ResourceRequest(RType.CPU_core, inf),       # GHz
            ResourceRequest(RType.RAM,      5),         # GB
            ResourceRequest(RType.GPU,      1024),      # nCC
        ]
        res2 = [
            ResourceRequest(RType.CPU_core, inf),       # GHz
            ResourceRequest(RType.RAM,      5),         # GB
            ResourceRequest(RType.GPU,      512),       # nCC
        ]
        job0 = Job(200, res0)
        job1 = Job(300, res1)
        job2 = Job(200, res2)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))
        sim.addEvent(0, JobStart(job1, m0))
        sim.addEvent(40, JobStart(job2, m0))

        inspector = EventInspector([
            {'time': 0, 'what': NType.JobStart, 'job': job0},
            {'time': 0, 'what': NType.JobStart, 'job': job1},
            {'time': 40, 'what': NType.JobFinish, 'job': job0},
            {'time': 40, 'what': NType.JobStart, 'job': job2},
            {'time': 60, 'what': NType.JobFinish, 'job': job1},
            {'time': 70, 'what': NType.JobFinish, 'job': job2},
        ])
        sim.simulate()
        inspector.verify()


    def test_gpuLength(self):
        inf = INF
        resources = {
            Resource(RType.CPU_core, 10),               # GHz
            Resource(RType.CPU_core, 10),               # GHz
            Resource(RType.RAM, 16),                    # GB
            Resource(RType.GPU, 1664, 1),               # nCC,GHz
        }
        m0 = Machine("m0", resources)

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

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0, m0))
        sim.addEvent(15, JobStart(job1, m0))

        inspector = EventInspector([
            {'time': 0, 'what': NType.JobStart, 'job': job0},
            {'time': 15, 'what': NType.JobFinish, 'job': job0},
            {'time': 15, 'what': NType.JobStart, 'job': job1},
            {'time': 35, 'what': NType.JobFinish, 'job': job1},
        ])
        sim.simulate()
        inspector.verify()

