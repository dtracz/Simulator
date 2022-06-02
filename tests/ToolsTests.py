import nose
from unittest import TestCase
from toolkit import INF
from BinPackingScheduler import Task, Timeline
from Job import *
from Machine import *
from Generator import CreateVM


class ToolsTests(TestCase):

    def setUp(self):
        Job._noCreated = 0
        Machine._noCreated = 0

    def tearDown(self):
        pass


    @staticmethod
    def getTask(length, noCores, ram):
        res = [ResourceRequest(Resource.Type.RAM, ram)]
        for _ in range(noCores):
            res += [ResourceRequest(Resource.Type.CPU_core, INF)]
        job = Job(length, res, )
        vm = CreateVM.minimal([job])
        vm.scheduleJob(job)
        return Task(vm)

    @staticmethod
    def sumTasks(tasks):
        sumDict = {}
        for task in tasks:
            for key, val in task.dims.items():
                if key not in sumDict.keys():
                    sumDict[key] = val
                else:
                    sumDict[key] += val
        return sumDict


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

            



