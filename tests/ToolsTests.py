import nose
from tests.base_test import *
from toolkit import INF
from Job import *
from Machine import *
from Generator import CreateVM
from scheduling.Task import *
from scheduling.Bins import TimelineBin
from scheduling.BinPackingScheduler import *


class ToolsTests(SimulatorTests):

    @staticmethod
    def getTask(length, noCores, ram, ownCores=True):
        res = [ResourceRequest(Resource.Type.RAM, ram)]
        for _ in range(noCores):
            res += [ResourceRequest(Resource.Type.CPU_core, INF)]
        job = Job(length, res, )
        vm = CreateVM.minimal([job], ownCores=ownCores)
        vm.scheduleJob(job)
        return Task(vm)


    def test_TimelineAdd(self):
        t0 = self.getTask(80, 2, 1)
        t1 = self.getTask(50, 1, 1)
        t2 = self.getTask(70, 1, 1)
        t3 = self.getTask(120,3, 1)
        t4 = self.getTask(40, 2, 1)

        tl = Timeline()
        tl.add(0,  t0)
        tl.add(20, t1)
        tl.add(40, t2)
        tl.add(40, t3)
        tl.add(70, t4)

        expTimes = [0, 20, 40, 70, 80, 90, 110]
        expTasks = [
            {t0},
            {t0, t1},
            {t1, t2, t3},
            {t2, t3, t4},
            {t2, t4},
            {t2},
            set(),
        ]

        for point, eTime, eTasks in zip(tl.timepoints(), expTimes, expTasks):
            assert point == eTime
            assert tl[point] == eTasks


    def test_TimelineRemove(self):
        t0 = self.getTask(80, 2, 1)
        t1 = self.getTask(50, 1, 1)
        t2 = self.getTask(70, 1, 1)
        t3 = self.getTask(120,3, 1)
        t4 = self.getTask(40, 2, 1)

        tl = Timeline()
        tl.add(0,  t0)
        tl.add(20, t1)
        tl.add(40, t2)
        tl.add(40, t3)
        tl.add(70, t4)

        tl.remove(t1)
        tl.remove(t3)

        expTimes = [0, 40, 70, 90, 110]
        expTasks = [
            {t0},
            {t2},
            {t2, t4},
            {t2},
            set(),
        ]

        for point, eTime, eTasks in zip(tl.timepoints(), expTimes, expTasks):
            assert point == eTime
            assert tl[point] == eTasks
            
        tl.remove(t0)
        tl.remove(t2)
        tl.remove(t4)

        assert len(tl.timepoints()) == 0


    def test_TimelineBin(self):
        tb = TimelineBin({
            RType.CPU_core: 4,
            RType.RAM: 16,
        })
        tb.add(self.getTask(80, 2, 1, True))
        tb.add(self.getTask(50, 1, 1, True))
        tb.add(self.getTask(70, 1, 1, True))
        tb.add(self.getTask(120,3, 1, True))
        tb.add(self.getTask(40, 2, 1, True))

        inspector = EventInspector()
        tl = tb._tasks
        seen = set()
        running = set()
        for tp in tl.timepoints():
            current = tl[tp]
            rm = []
            for task in running:
                if task not in current:
                    inspector.addExpectation(time=tp, what=NType.JobFinish, job=task.job)
                    rm += [task]
            for task in rm:
                running.remove(task)
            for task in current:
                if task not in seen:
                    inspector.addExpectation(time=tp, what=NType.JobStart, job=task.job)
                    seen.add(task)
                    running.add(task)

        resources = {
            Resource(Resource.Type.CPU_core, 1), # GHz
            Resource(Resource.Type.CPU_core, 1), # GHz
            Resource(Resource.Type.CPU_core, 1), # GHz
            Resource(Resource.Type.CPU_core, 1), # GHz
            Resource(Resource.Type.RAM, 16),     # GB
        }
        m0 = Machine("m0", resources, lambda m: None, BinPackingScheduler)
        m0._vmScheduler._bins = [tb]

        sim = Simulator.getInstance()
        sim.simulate()

        inspector.verify()

