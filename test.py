import nose
from unittest import TestCase
from Simulator import *
from Machine import *
from Job import *
 


class SimulatorTests(TestCase):

    def setup(self):
        Simulator.getInstance()

    def teardown(self):
        sim = Simulator.getInstance()
        del sim



class SimpleTests(SimulatorTests):
 
    def test_1job(self):
        resources = {
            "Core 0": Resource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 16),  # GB
        }
        m0 = Machine("m0", resources)

        res0 = {
            "Core 0": Resource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 5),  # GB
        }
        job0 = Job(100, res0, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))

        sim.simulate()
        assert(sim.time == 10)


    def test_2jobs2cores(self):
        resources = {
            "Core 0": Resource("Core 0", 10), # GHz
            "Core 1": Resource("Core 1", 10), # GHz
            "RAM"   : Resource("RAM", 16),  # GB
        }
        m0 = Machine("m0", resources)

        res0 = {
            "Core 0": Resource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 5),  # GB
        }
        res1 = {
            "Core 1": Resource("Core 1", 10), # GHz
            "RAM"   : Resource("RAM", 8),  # GB
        }
        job0 = Job(650, res0, m0)
        job1 = Job(450, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        sim.simulate()
        assert(sim.time == 65)


    def test_ramFailure(self):
        resources = {
            "Core 0": Resource("Core 0", 10), # GHz
            "Core 1": Resource("Core 1", 10), # GHz
            "RAM"   : Resource("RAM", 16),  # GB
        }
        m0 = Machine("m0", resources)

        res0 = {
            "Core 0": Resource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 10),  # GB
        }
        res1 = {
            "Core 1": Resource("Core 1", 10), # GHz
            "RAM"   : Resource("RAM", 8),  # GB
        }
        job0 = Job(650, res0, m0)
        job1 = Job(450, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        cought = False
        try:
            sim.simulate()
        except RuntimeError as e:
            cought = e.args[0] == 'Requested 8 out of 6 avaliable'
        assert(cought)


 
class SharedResourceTests(SimulatorTests):

    def test_1coreSimple(self):
        inf = float('inf')
        resources = {
            "Core 0": SharedResource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 16),  # GB
        }
        m0 = Machine("m0", resources)

        res0 = {
            "Core 0": Resource("Core 0", inf), # GHz
            "RAM"   : Resource("RAM", 5),  # GB
        }
        res1 = {
            "Core 0": Resource("Core 0", inf), # GHz
            "RAM"   : Resource("RAM", 8),  # GB
        }
        job0 = Job(200, res0, m0)
        job1 = Job(200, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        sim.simulate()
        assert(sim.time == 40)


    def test_1vs2(self):
        inf = float('inf')
        resources = {
            "Core 0": SharedResource("Core 0", 10), # GHz
            "Core 1": SharedResource("Core 1", 10), # GHz
            "RAM"   : Resource("RAM", 16),  # GB
        }
        m0 = Machine("m0", resources)

        res0 = {
            "Core 0": Resource("Core 0", inf), # GHz
            "Core 1": Resource("Core 1", inf), # GHz
            "RAM"   : Resource("RAM", 5),  # GB
        }
        res1 = {
            "Core 1": Resource("Core 1", inf), # GHz
            "RAM"   : Resource("RAM", 8),  # GB
        }
        job0 = Job(600, res0, m0)
        job1 = Job(400, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(30, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        sim.simulate()
        assert(sim.time == 65)


