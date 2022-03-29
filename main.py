from Simulator import *
from Event import *
from Machine import *

if __name__ == "__main__":
    resources = {
        "Core 0": Resource(3.2), # GHz
        "Core 1": Resource(3.2), # GHz
        "RAM"   : Resource(16),  # GB
    }
    m0 = Machine("m0", resources)

    res0 = {
        "Core 0": Resource(3.2), # GHz
        "RAM"   : Resource(5),  # GB
    }
    job0 = Job(m0, 650, res0)
    res1 = {
        "Core 1": Resource(3.2), # GHz
        "RAM"   : Resource(8),  # GB
    }
    job1 = Job(m0, 450, res1)
    
    sim = Simulator.getInstance()
    sim.addEvent(0, job0)
    sim.addEvent(0, job1)

    sim.simulate()
    print(f"done in {sim.time} s")




