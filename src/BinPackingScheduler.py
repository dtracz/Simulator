from sortedcontainers import SortedDict
from toolkit import *
from Simulator import *
from Events import *
from Resource import *
from Machine import *
from Job import *
from Schedulers import *


class Task:
    def __init__(self, vm):
        self.vm = vm
        assert vm._jobScheduler is not None
        assert len(vm._jobScheduler._jobQueue) == 1
        self.job = vm._jobScheduler._jobQueue[0]
        self.dims = {}
        rams = list(filter(lambda rv: rv[0] == Resource.Type.RAM,
                           vm.maxResources))
        assert len(rams) == 1
        self.dims[rams[0][0]] = rams[0][1]
        cores = list(filter(lambda rv: rv[0] == Resource.Type.CPU_core,
                            vm.maxResources))
        assert len(cores) > 0
        self.dims[cores[0][0]] = len(cores)
        self.startpoint = None

    def reduceCores(self, n=-1):
        noCores = self.dims[Resource.Type.CPU_core]
        if n <= 0:
            n += noCores
        if n <= 0:
            raise Exception("Task has to have at leat 1 core assigned")
        if n < noCores:
            self.dims[Resource.Type.CPU_core] = noCores
        return noCores

    def restoreCores(self, n=INF):
        maxCores = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                                  self.job.resourceRequest)))
        currentCores = self.dims[Resource.Type.CPU_core]
        self.dims[Resource.Type.CPU_core] = min(maxCores, currentCores + n)
        return self.dims[Resource.Type.CPU_core]

    @property
    def length(self):
        ops = self.job.operations
        noCores = self.dims[Resource.Type.CPU_core]
        return ops / noCores

    @property
    def endpoint(self):
        return None if self.startpoint is None else self.startpoint + self.length

    @staticmethod
    def sum(tasks):
        sumDict = {}
        for task in tasks:
            for key, val in task.dims.items():
                if key not in sumDict.keys():
                    sumDict[key] = val
                else:
                    sumDict[key] += val
        return sumDict



class Timeline:
    def __init__(self):
        self._dict = Map()

    def _completePoint(self, time):
        assert time in self.timepoints()
        if self._dict.peekitem(0)[0] < time:
            time_ = self._dict.first_key_lower(time)
            tasks = self._dict[time_]
            for task in tasks:
                if task.endpoint > time:
                    self._dict[time].add(task)

    def _clearPoint(self, time):
        assert time in self.timepoints()
        thisPoint = self._dict[time]
        prevPoint = set()
        if self._dict.peekitem(0)[0] < time:
            time_ = self._dict.first_key_lower(time)
            prevPoint = self._dict[time_]
        if prevPoint == thisPoint:# and \
            del self._dict[time]

    def add(self, time, task):
        if time not in self._dict.keys():
            self._dict[time] = set()
            self._completePoint(time)
        if time + task.length not in self._dict.keys():
            self._dict[time + task.length] = set()
            self._completePoint(time + task.length)
        for t in self._dict.irange(time, time + task.length, (True, False)):
            self._dict[t].add(task)
        task.startpoint = time

    def remove(self, task):
        for t in self._dict.irange(task.startpoint, task.endpoint, (True, False)):
            self._dict[t].remove(task)
        self._clearPoint(task.startpoint)
        self._clearPoint(task.endpoint)
        task.startpoint = None

    def __getitem__(self, time):
        assert time >= 0
        if time in self._dict.keys():
            return self._dict[time]
        time_ = self._dict.first_key_lower(time)
        return self._dict[time_]

    def timepoints(self):
        return self._dict.keys()



class SimpleBin:
    def __init__(self, maxDims):
        self.maxDims = maxDims # {Resource.Type: maxSize}
        self._tasks = set()
        self._closed = False

    @property
    def length(self):
        longest = max(self._tasks, key=lambda t: t.length)
        return longest.length

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
        if task not in self._tasks:
            raise KeyError(f"{self} does not contain {task}")
        self._tasks.remove(task)

    def efficiency(self, tasks=None):
        if tasks is None:
            tasks = self._tasks
        rtypes = self.maxDims.keys()
        length = self.length
        eff = {}
        for rtype in rtypes:
            usage = 0
            for task in self._tasks:
                usage += task.length * task.dims[rtype]
            eff[rtype] = usage / (self.maxDims[rtype] * length)
        return eff

    def close(self):
        self._closed = True
        self._tasks = list(self._tasks)
        self._tasks.sort(key=lambda t: t.job._index)

    def getNext(self):
        if not self._closed:
            raise Exception("Bin not closed yet")
        if len(self._tasks) == 0:
            return None
        return self._tasks[0].vm

    def popNext(self):
        if not self._closed:
            raise Exception("Bin not closed yet")
        return self._tasks.pop(0).vm



class ReductiveBin(SimpleBin):

    def _reduceOne(self):
        bestToReduce = None
        lengthOverhead = INF
        for task in self._tasks:
            noCores = task.dims[Resource.Type.CPU_core]
            if noCores < 2:
                continue
            futureLength = (task.length * noCores) / (noCores - 1)
            if futureLength - self.length < lengthOverhead:
                lengthOverhead = futureLength - self.length
                bestToReduce = task
        if bestToReduce is not None:
            bestToReduce.reduce()
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
        while self.currentDims[Resource.Type.CPU_core] > self.maxDims[Resource.Type.CPU_core]:
            redTask = self._reduceOne()
            if redTask is None:
                self._restoreReduced(reduced)
                return False
            if redTask not in reduced.keys():
                reduced[redTask] = 0
            reduced[redTask] += 1
        return True



class BinPackingScheduler(VMSchedulerSimple):
    """
    Schedules virtual machines on single hardware machine
    Assumptions:
    -> VM contains exactly one job
    -> VM requests for at most the same
       amount of resources that job
    """


    def __init__(self, machine):
        super().__init__(machine)
        self._maxDims = {}
        rams = list(filter(lambda r: r.rtype == Resource.Type.RAM,
                           machine.resources))
        assert len(rams) == 1
        self._maxDims[rams[0].rtype] = rams[0].value
        cores = list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                            machine.resources))
        assert len(cores) > 0
        self._maxDims[cores[0].rtype] = len(cores)
        self._bins = []
        self._currentBin = None

    def _loadNextBin(self):
        if len(self._bins) == 0:
            return False
        self._currentBin = max(self._bins,
                               key=lambda b: b.efficiency()[Resource.Type.CPU_core])
        idx = self._bins.index(self._currentBin)
        del self._bins[idx]
        self._currentBin.close()
        return True

    def head(self):
        if self._currentBin is None:
            if not self._loadNextBin():
                return None
        vm = self._currentBin.getNext()
        if vm is None:
            if not self._loadNextBin():
                return None
            vm = self._currentBin.getNext()
        return vm

    def popFront(self):
        return self._currentBin.popNext()

    @staticmethod
    def evalFitting(bucket, task):
        lgthDiff = abs(bucket.length - task.length) / bucket.length
        if lgthDiff > 0.3:
            return None
        eff0 = bucket.efficiency()
        if not bucket.add(task):
            return None
        eff1 = bucket.efficiency()
        bucket.remove(task)
        dEff = dictMinus(eff0, eff1)
        return dictMultiply(1 - lgthDiff, dEff)

    def schedule(self, vm):
        if not self._machine.isFittable(vm):
            raise Exception(f"{vm.name} can never be allocated"
                            f" on {self._machine.name}")
        task = Task(vm)
        bestBucket = None
        bestScore = {}
        for key in self._maxDims.keys():
            bestScore[key] = -INF
        for bucket in self._bins:
            score = self.evalFitting(bucket, task)
            if score is None:
                continue
            if score[Resource.Type.CPU_core] > bestScore[Resource.Type.CPU_core]:
                bestBucket = bucket
                bestScore = score
        if bestBucket is None:
            self._bins += [SimpleBin(self._maxDims)]
            bestBucket = self._bins[-1]
        if not bestBucket.add(task):
            raise Exception(f"{vm.name} cannot be fit into any bucket")

