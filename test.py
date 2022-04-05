import nose
from Simulator import *
from Machine import *
from Job import *
 

class SimulatorTests:

    def setup(self):
        Simulator.getInstance()

    def teardown(self):
        sim = Simulator.getInstance()
        del sim


class test_class_1(SimulatorTests):
 
 
    def test_case_2(self):
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


 
