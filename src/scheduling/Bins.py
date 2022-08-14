from itertools import permutations
from toolkit import *
from Simulator import *
from Events import *
from Resource import *
from Machine import *
from Job import *
from scheduling.Task import *


class SimpleBin:
    def __init__(self, maxDims):
        self.maxDims = maxDims # {RType: maxSize}
        self._tasks = set()
        self._closed = False

    @property
    def length(self):
        longest = max(self._tasks, key=lambda t: t.length)
        return longest.length

    @property
    def vms(self):
        return [task.vm for task in self._tasks]

    @property
    def currentDims(self):
        dims = {}
        for rtype in self.maxDims.keys():
            dims[rtype] = 0
        for task in self._tasks:
            for rtype, value in task.dims.items():
                dims[rtype] += value
        return dims

    def add(self, task):
        if self._closed:
            raise Exception("Bin already closed")
        for rtype, limit in self.maxDims.items():
            assert rtype in task.dims
            assert rtype in self.maxDims
            if task.dims[rtype] + self.currentDims[rtype] > limit:
                return False
        self._tasks.add(task)
        return True

    def remove(self, task):
        if self._closed:
            raise Exception("Bin already closed")
        self._tasks.remove(task)

    def efficiency(self, tasks=None):
        if tasks is None:
            tasks = self._tasks
        rtypes = self.maxDims.keys()
        length = self.length
        eff = {}
        for rtype in rtypes:
            usage = 0
            for task in tasks:
                usage += task.length * task.dims[rtype]
            eff[rtype] = usage / (self.maxDims[rtype] * length)
        return eff

    def close(self):
        self._tasks = list(self._tasks)
        self._tasks.sort(key=lambda t: t.job._index)
        self._closed = True

    def getNext(self):
        if not self._closed:
            raise Exception("Bin not closed yet")
        if len(self._tasks) == 0:
            return None
        return self._tasks[0]

    def popNext(self):
        if not self._closed:
            raise Exception("Bin not closed yet")
        return self._tasks.pop(0)



class ReductiveBin(SimpleBin):

    def _reduceOne(self):
        bestToReduce = None
        lengthOverhead = INF
        for task in self._tasks:
            noCores = task.dims[RType.CPU_core]
            if noCores < 2:
                continue
            futureLength = (task.length * noCores) / (noCores - 1)
            if futureLength - self.length < lengthOverhead:
                lengthOverhead = futureLength - self.length
                bestToReduce = task
        if bestToReduce is not None:
            bestToReduce.reduceCores()
        return bestToReduce

    def _restoreReduced(self, reduced=None):
        if reduced == None:
            for task in self._tasks:
                task.restoreCores()
        else:
            for task, n in reduced.items():
                task.restoreCores(n)

    def _refitJobs(self):
        reduced = {}
        while self.currentDims[RType.CPU_core] > self.maxDims[RType.CPU_core]:
            redTask = self._reduceOne()
            if redTask is None:
                self._restoreReduced(reduced)
                return False
            if redTask not in reduced.keys():
                reduced[redTask] = 0
            reduced[redTask] += 1
        return True

    def add(self, task):
        if self._closed:
            raise Exception("Bin already closed")
        for rtype, limit in self.maxDims.items():
            if rtype == RType.CPU_core:
                continue
            assert rtype in task.dims
            assert rtype in self.maxDims
            if task.dims[rtype] + self.currentDims[rtype] > limit:
                return False
        self._tasks.add(task)
        if self._refitJobs():
            return True
        self._tasks.remove(task)
        return False

    def remove(self, task):
        super().remove(task)
        self._restoreReduced()
        assert self._refitJobs()



class TimelineBin(SimpleBin):
    def __init__(self, maxDims):
        super().__init__(maxDims)
        self._tasks = Timeline()

    @property
    def length(self):
        tp = self._tasks.timepoints()
        return tp[-1] - tp[0]

    @property
    def vms(self):
        return [task.vm for task in self._tasks.allTasks]

    @property
    def currentDims(self):
        raise NotImplementedError("TimelineBin has no 'currentDims'")

    @staticmethod
    def addToTimeline(tasks, maxDims, task):
        timepoints = tasks.timepoints()
        if len(timepoints) == 0:
            tasks.add(0, task)
            return True
        lastOK = None
        for time in timepoints:
            tasksAt = tasks[time]
            occupied = Task.sum(tasksAt)
            allOK = True
            for rtype, limit in maxDims.items():
                allOK *= occupied.get(rtype, 0) + task.dims[rtype] \
                      <= maxDims[rtype]
            if allOK:
                if lastOK is None:
                    lastOK = time
                elif time - lastOK >= task.length:
                    break
            else:
                lastOK = None
        assert lastOK is not None
        tasks.add(lastOK, task)
        return True

    def add(self, task):
        if self._closed:
            raise Exception("Bin already closed")
        for rtype, limit in self.maxDims.items():
            if task.dims[rtype] > limit:
                return False
        return self.addToTimeline(self._tasks, self.maxDims, task)

    def close(self):
        self._tasks = list(self._tasks)
        self._closed = True



class OrderedTimelineBin(TimelineBin):
    def __init__(self, maxDims):
        super().__init__(maxDims)
        self._prevTl = None

    @staticmethod
    def _orderAdd(timeline, maxDims):
        return timeline

    @staticmethod
    def _orderRemove(timeline, maxDims):
        return timeline

    @staticmethod
    def _orderClose(timeline, maxDims):
        return timeline

    def add(self, task):
        if self._closed:
            raise Exception("Bin already closed")
        for rtype, limit in self.maxDims.items():
            if task.dims[rtype] > limit:
                return False
        prevTl = self._prevTl
        self._prevTl = self._tasks.copy()
        assert self.addToTimeline(self._tasks, self.maxDims, task)
        self._tasks = self._orderAdd(self._tasks, self.maxDims)
        return True

    def remove(self, task):
        if self._closed:
            raise Exception("Bin already closed")
        if self._prevTl is not None:
            diff = self._tasks.allTasks.difference(self._prevTl.allTasks)
            if len(diff) == 1 and task in diff:
                self._tasks = self._prevTl
                self._prevTl = None
                return
        self._tasks.remove(task)
        self._tasks = self._orderRemove(self._tasks, self.maxDims)
        self._prevTl = None

    def close(self):
        tasks = self._orderClose(self._tasks, self.maxDims)
        self._tasks = list(tasks)
        self._closed = True



def orderLongestFirst(timeline, maxDims):
    tasks = list(timeline.allTasks)
    tasks.sort(key=lambda t: -t.length)
    new_tl = Timeline()
    for task in tasks:
        assert TimelineBin.addToTimeline(new_tl, maxDims, task)
    return new_tl


def orderExhausive(timeline, maxDims):
    tasks = list(timeline.allTasks)
    best_tl = None
    best_score = INF
    for tasksPerm in permutations(tasks):
        new_tl = Timeline()
        for task in tasksPerm:
            assert TimelineBin.addToTimeline(new_tl, maxDims, task)
        if new_tl.timepoints()[-1] - new_tl.timepoints()[0] < best_score:
            best_tl = new_tl
    return best_tl


def OrderedTimelineBinClass(orderAdd, orderRemove, orderClose):
    class _OrderedTimelineBin(OrderedTimelineBin):
        @staticmethod
        def _orderAdd(timeline, maxDims):
            return orderAdd(timeline, maxDims)

        @staticmethod
        def _orderRemove(timeline, maxDims):
            return orderRemove(timeline, maxDims)

        @staticmethod
        def _orderClose(timeline, maxDims):
            return orderClose(timeline, maxDims)

    return _OrderedTimelineBin

