import nose
from unittest import TestCase
from Simulator import *
from Resource import *
from Machine import *
from Job import *
from Schedulers import *
 

class EventInspector(NotificationListener):
    def __init__(self, expected=[]):
        self._expectations = []
        for time, name in expected:
            self.addExpected(time, name)

    def addExpected(self, time, name):
        self._expectations += [
            lambda e: e.name == name and \
                      Simulator.getInstance().time == time,
        ]

    def notify(self, event):
        for i, f in enumerate(self._expectations):
            if f(event):
                del self._expectations[i]
                break

    def verify(self):
        assert 0 == len(self._expectations)



class SimulatorTests(TestCase):

    def setUp(self):
        sim = Simulator.getInstance()
        assert sim.time == 0
        assert len(sim._eventQueue._todo) == 1
        assert len(sim._eventQueue._done) == 0

    def tearDown(self):
        sim = Simulator.getInstance()
        sim.clear()



class SimpleTests(SimulatorTests):
 
    def test_1job(self):
        resources = {
            Resource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),    # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            (Resource.Type.CPU_core, 10), # GHz
            (Resource.Type.RAM,      5),  # GB
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
            (Resource.Type.CPU_core, 10), # GHz
            (Resource.Type.RAM,      5),  # GB
        ]
        res1 = [
            (Resource.Type.CPU_core, 10), # GHz
            (Resource.Type.RAM,      8),  # GB
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
            (Resource.Type.CPU_core, 10), # GHz
            (Resource.Type.CPU_core, 5),  # GHz
            (Resource.Type.RAM,      5),  # GB
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
            (Resource.Type.CPU_core, 10), # GHz
            (Resource.Type.RAM,      10),  # GB
        ]
        res1 = [
            (Resource.Type.CPU_core, 10), # GHz
            (Resource.Type.RAM,      8),  # GB
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
            cought = e.args[0] == 'Requested 8 out of 6 avaliable'
        assert cought


 
class SharedResourceTests(SimulatorTests):

    def test_1coreSimple(self):
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            (Resource.Type.CPU_core, inf), # GHz
            (Resource.Type.RAM,      5),   # GB
        ]
        res1 = [
            (Resource.Type.CPU_core, inf), # GHz
            (Resource.Type.RAM,      8),   # GB
        ]
        job0 = Job(200, res0, m0)
        job1 = Job(200, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        sim.simulate()
        assert sim.time == 40


    def test_1vs2(self):
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        res0 = [
            (Resource.Type.CPU_core, inf), # GHz
            (Resource.Type.CPU_core, inf), # GHz
            (Resource.Type.RAM,      5),   # GB
        ]
        res1 = [
            (Resource.Type.CPU_core, inf), # GHz
            (Resource.Type.RAM,      8),   # GB
        ]
        job0 = Job(600, res0, m0)
        job1 = Job(400, res1, m0)

        sim = Simulator.getInstance()
        sim.addEvent(30, JobStart(job0))
        sim.addEvent(0, JobStart(job1))

        sim.simulate()
        assert sim.time == 65



class VirtualizationTests(SimulatorTests):

    def test_allocateVM(self):
        inf = float('inf')
        resources = {
            "Core 0": SharedResource("Core 0", 10), # GHz
            "Core 1": SharedResource("Core 1", 10), # GHz
            "RAM"   : Resource("RAM", 16),          # GB
        }
        m0 = Machine("m0", resources)
        resourceReq0 = {
            "Core 0": inf, # GHz
            "Core 1": inf, # GHz
            "RAM"   : 10,  # GB
        }
        vm0 = VirtualMachine("vm0", resourceReq0)
        m0.allocateVM(vm0)
        assert m0._resources["Core 0"].value == 10
        assert m0._resources["Core 1"].value == 10
        assert m0._resources["RAM"].value == 6
        assert vm0._resources["Core 0"].value == 10
        assert vm0._resources["Core 1"].value == 10
        assert vm0._resources["RAM"].value == 10
        m0.freeVM(vm0)
        assert m0._resources["Core 0"].value == 10
        assert m0._resources["Core 1"].value == 10
        assert m0._resources["RAM"].value == 16
        assert len(vm0._resources) == 0


    def test_2jobsOn2VMs(self):
        inf = float('inf')
        resources = {
            "Core 0": SharedResource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 16),          # GB
        }
        m0 = Machine("m0", resources)
        resourceReq0 = {
            "Core 0": inf, # GHz
            "RAM"   : 8,   # GB
        }
        vm0 = VirtualMachine("vm0", resourceReq0)
        resourceReq1 = {
            "Core 0": inf, # GHz
            "RAM"   : 8,   # GB
        }
        vm1 = VirtualMachine("vm1", resourceReq0)
        m0.allocateVM(vm0)
        m0.allocateVM(vm1)
        job0 = Job(100, {"Core 0": inf, "RAM": 2}, vm0)
        job1 = Job(100, {"Core 0": inf, "RAM": 2}, vm1)

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(1, JobStart(job1))
        sim.simulate()

        m0.freeVM(vm0)
        m0.freeVM(vm1)

        assert sim.time == 20



class SchedulersTests(SimulatorTests):

    def test_jobSchedulerSimple(self):
        inf = float('inf')
        resources = {
            "Core 0": SharedResource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 16),          # GB
        }
        m0 = Machine("m0", resources)

        resourceReq0 = {
            "Core 0": inf, # GHz
            "RAM"   : inf, # GB
        }
        vm0 = VirtualMachine("vm0", resourceReq0,
                lambda machine: JobSchedulerSimple(machine, autofree=True))

        job0 = Job(500, {"Core 0": inf, "RAM": 8}, vm0)
        job1 = Job(1000, {"Core 0": inf, "RAM": 6}, vm0)
        job2 = Job(1000, {"Core 0": inf, "RAM": 6}, vm0)

        vm0.scheduleJob(job0)
        vm0.scheduleJob(job1)
        vm0.scheduleJob(job2)

        sim = Simulator.getInstance()
        sim.addEvent(0, VMStart(m0, vm0))
        sim.simulate()

        assert sim.time == 250


    def test_vmSchedulerSimple(self):
        inf = float('inf')
        resources = {
            "Core 0": SharedResource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 16),          # GB
        }
        m0 = Machine("m0", resources, lambda m: None, VMSchedulerSimple)

        def getVM(vm_id):
            resourceReq = {
                "Core 0": inf, # GHz
                "RAM"   : 8, # GB
            }

            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            job0 = Job(500, {"Core 0": inf, "RAM": 8}, vm)
            job1 = Job(1000, {"Core 0": inf, "RAM": 6}, vm)
            job2 = Job(1000, {"Core 0": inf, "RAM": 6}, vm)
            vm.scheduleJob(job0)
            vm.scheduleJob(job1)
            vm.scheduleJob(job2)
            return vm

        m0.scheduleVM(getVM(0))
        m0.scheduleVM(getVM(1))
        m0.scheduleVM(getVM(2))
        m0.scheduleVM(getVM(3))

        sim = Simulator.getInstance()
        sim.simulate()

        assert sim.time == 1000


    def test_placementPolicySimple(self):
        inf = float('inf')
        resources = {
            "Core 0": SharedResource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 8),           # GB
        }
        m0 = Machine("m0", resources, lambda m: None, VMSchedulerSimple)
        resources = {
            "Core 0": SharedResource("Core 0", 10), # GHz
            "RAM"   : Resource("RAM", 16),          # GB
        }
        m1 = Machine("m1", resources, lambda m: None, VMSchedulerSimple)
        
        infrastructure = Infrastructure.getInstance(
                [m0, m1],
                VMPlacmentPolicySimple,
        )
    
        def getVM(vm_id, ram, req_jobs):
            resourceReq = {
                "Core 0": inf, # GHz
                "RAM"   : ram, # GB
            }
            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            jobs = [
                Job(500, {"Core 0": inf, "RAM": 8}, vm),
                Job(1000, {"Core 0": inf, "RAM": 6}, vm),
                Job(1000, {"Core 0": inf, "RAM": 6}, vm),
            ]
            for i in req_jobs:
                vm.scheduleJob(jobs[i])
            return vm

        infrastructure.scheduleVM(getVM(0, 8, [0]))
        infrastructure.scheduleVM(getVM(0, 8, [1,2]))
        infrastructure.scheduleVM(getVM(0, 16, [0,1,2]))
        infrastructure.scheduleVM(getVM(0, 12, [1,2]))

        sim = Simulator.getInstance()
        sim.simulate()
        assert sim.time == 650

