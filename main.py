from Simulator import *
from Event import *
from Machine import *
from Job import *

if __name__ == "__main__":
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
    sim.addEvent(20, JobStart(job0))
    sim.addEvent(0, JobStart(job1))

    sim.simulate()
    print(f"done in {sim.time} s")




    #  resources = {
    #      "Core 0": Resource("Core 0", 3.2), # GHz
    #      "Core 1": Resource("Core 1", 3.2), # GHz
    #      "RAM"   : Resource("RAM", 16),  # GB
    #  }
    #  m0 = Machine("m0", resources)
    #
    #  res0 = {
    #      "Core 0": Resource("Core 0", 3.2), # GHz
    #      "RAM"   : Resource("RAM", 5),  # GB
    #  }
    #  res1 = {
    #      "Core 1": Resource("Core 1", 3.2), # GHz
    #      "RAM"   : Resource("RAM", 8),  # GB
    #  }
    #  job0 = Job(650, res0, m0)
    #  job1 = Job(450, res1, m0)
    #
    #  sim = Simulator.getInstance()
    #  sim.addEvent(0, JobStart(job0))
    #  sim.addEvent(0, JobStart(job1))
    #
    #  sim.simulate()
    #  print(f"done in {sim.time} s")