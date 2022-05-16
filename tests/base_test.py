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
        Event._noCreated = 0
        Job._noCreated = 0
        Machine._noCreated = 0
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
                     e.args[0] == "Cannot find fitting Type.RAM"
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
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            SharedResource(Resource.Type.CPU_core, 10), # GHz
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
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
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



class VirtualizationTests(SimulatorTests):

    def test_allocateVM(self):
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)
        resourceReq0 = {
            ResourceRequest(Resource.Type.CPU_core, inf),
            ResourceRequest(Resource.Type.CPU_core, inf),
            ResourceRequest(Resource.Type.RAM, 5),
        }
        vm0 = VirtualMachine("vm0", resourceReq0)
        m0.allocateVM(vm0)
        assert m0._resources.getAll(Resource.Type.CPU_core)[0].avaliableValue == 0
        assert m0._resources.getAll(Resource.Type.CPU_core)[1].avaliableValue == 0
        assert m0._resources.getAll(Resource.Type.RAM)[0].avaliableValue == 11
        assert vm0._resources.getAll(Resource.Type.CPU_core)[0].maxValue == 10
        assert vm0._resources.getAll(Resource.Type.CPU_core)[1].maxValue == 10
        assert vm0._resources.getAll(Resource.Type.RAM)[0].maxValue == 5

    def test_freeVM(self):
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)
        resourceReq0 = {
            ResourceRequest(Resource.Type.CPU_core, inf),
            ResourceRequest(Resource.Type.CPU_core, inf),
            ResourceRequest(Resource.Type.RAM, 5),
        }
        vm0 = VirtualMachine("vm0", resourceReq0)
        m0.allocateVM(vm0)
        m0.freeVM(vm0)
        assert m0._resources.getAll(Resource.Type.CPU_core)[0].avaliableValue == 10
        assert isinstance(m0._resources.getAll(Resource.Type.CPU_core)[0], SharedResource)
        assert m0._resources.getAll(Resource.Type.CPU_core)[1].avaliableValue == 10
        assert isinstance(m0._resources.getAll(Resource.Type.CPU_core)[1], SharedResource)
        assert m0._resources.getAll(Resource.Type.RAM)[0].avaliableValue == 16
        assert 0 == len(vm0._resources)


    def test_2jobsOn2VMs(self):
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)
        resourceReq0 = {
            ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
            ResourceRequest(Resource.Type.RAM, 8),
        }
        vm0 = VirtualMachine("vm0", resourceReq0)
        resourceReq1 = {
            ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
            ResourceRequest(Resource.Type.RAM, 8),
        }
        vm1 = VirtualMachine("vm1", resourceReq0)
        m0.allocateVM(vm0)
        m0.allocateVM(vm1)

        job0 = Job(100,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 5)],
                   vm0
               )
        job1 = Job(100,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 5)],
                   vm1
               )

        sim = Simulator.getInstance()
        sim.addEvent(0, JobStart(job0))
        sim.addEvent(0, JobStart(job1))
        inspector = EventInspector([
            (0, "JobStart_Job_0"),
            (0, "JobStart_Job_1"),
            (20, "JobFinish_Job_0"),
            (20, "JobFinish_Job_1"),
        ])
        sim.simulate()
        inspector.verify()

        m0.freeVM(vm0)
        m0.freeVM(vm1)




class SchedulersTests(SimulatorTests):

    def test_jobSchedulerSimple(self):
        inf = float('inf')
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources)

        resourceReq0 = {
            ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
            ResourceRequest(Resource.Type.RAM, inf),
        }
        vm0 = VirtualMachine("vm0", resourceReq0,
                lambda machine: JobSchedulerSimple(machine, autofree=True))

        job0 = Job(500,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 8)],
                   vm0
               )
        job1 = Job(1000,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 6)],
                   vm0
               )
        job2 = Job(1000,
                   [ResourceRequest(Resource.Type.CPU_core, inf),
                    ResourceRequest(Resource.Type.RAM, 6)],
                   vm0
               )

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
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m0 = Machine("m0", resources, lambda m: None, VMSchedulerSimple)

        def getVM(vm_id):
            resourceReq = {
                ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
                ResourceRequest(Resource.Type.RAM, 8),
            }

            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            job0 = Job(500,
                       [ResourceRequest(Resource.Type.CPU_core, inf),
                        ResourceRequest(Resource.Type.RAM, 8)],
                       vm
                   )
            job1 = Job(1000,
                       [ResourceRequest(Resource.Type.CPU_core, inf),
                        ResourceRequest(Resource.Type.RAM, 6)],
                       vm
                   )
            job2 = Job(1000,
                       [ResourceRequest(Resource.Type.CPU_core, inf),
                        ResourceRequest(Resource.Type.RAM, 6)],
                       vm
                   )
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
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 8),            # GB
        }
        m0 = Machine("m0", resources, lambda m: None, VMSchedulerSimple)
        resources = {
            SharedResource(Resource.Type.CPU_core, 10), # GHz
            Resource(Resource.Type.RAM, 16),            # GB
        }
        m1 = Machine("m1", resources, lambda m: None, VMSchedulerSimple)

        infrastructure = Infrastructure.getInstance(
                [m0, m1],
                VMPlacmentPolicySimple,
        )

        def getVM(vm_id, ram, req_jobs):
            resourceReq = {
                ResourceRequest(Resource.Type.CPU_core, inf, shared=True),
                ResourceRequest(Resource.Type.RAM, ram),
            }
            vm = VirtualMachine(f"vm{vm_id}", resourceReq,
                    lambda machine: JobSchedulerSimple(machine, autofree=True))
            jobs = [
                Job(500,
                    [ResourceRequest(Resource.Type.CPU_core, inf),
                     ResourceRequest(Resource.Type.RAM, 8)],
                    vm
                ),
                Job(1000,
                    [ResourceRequest(Resource.Type.CPU_core, inf),
                     ResourceRequest(Resource.Type.RAM, 6)],
                    vm
                ),
                Job(1000,
                    [ResourceRequest(Resource.Type.CPU_core, inf),
                     ResourceRequest(Resource.Type.RAM, 6)],
                    vm
                ),
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

