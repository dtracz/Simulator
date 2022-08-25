from toolkit import *
from Simulator import *
from Events import *
from Resource import *
from Machine import *
from Job import *
from scheduling.BaseSchedulers import *
from scheduling.Task import *
from scheduling.Bins import *


class BinPackingScheduler(VMSchedulerSimple):
    """
    Schedules virtual machines on single hardware machine
    Assumptions:
    -> VM contains exactly one job
    -> VM requests for at most the same
       amount of resources that job
    """


    def __init__(self, machine, BinClass=SimpleBin, awaitBins=False):
        super().__init__(machine)
        self.BinClass = BinClass
        self._maxDims = {}
        for res in machine.resources:
            if res.rtype not in self._maxDims.keys():
                self._maxDims[res.rtype] = []
            self._maxDims[res.rtype] += [res]
        self._host_freqs = {}
        for rtype, res in self._maxDims.items():
            if rtype is RType.RAM:
                assert len(res) == 1
                self._maxDims[rtype] = res[0].maxValue
            if rtype is RType.CPU_core:
                assert len(res) > 0
                self._maxDims[rtype] = len(res)
                self._host_freqs[rtype] = min(res, key=lambda r: r.maxValue).maxValue
            if rtype is RType.GPU:
                self._maxDims[rtype] = len(res)
                self._host_freqs[rtype] = min(res, key=lambda r: r.freq).freq
        gpus = list(filter(lambda rv: rv[0] == RType.GPU,
                           self._machine.maxResources))
        self._gpu_min_nCC = min(list(zip(*gpus))[1]) if len(gpus) > 0 else None
        self._bins = []
        self._currentBin = None
        self._listener = EventInspector() if awaitBins else None

    @property
    def noVMsLeft(self):
        n = 0
        for bucket in self._bins:
            n += len(bucket._tasks)
        return n

    @property
    def vms(self):
        vms = []
        for bucket in self._bins:
            vms += bucket.vms
        return vms

    def _loadNextBin(self):
        if self._listener is not None and not self._listener.allRegistered():
            return False
        if len(self._bins) == 0:
            return False
        self._currentBin = max(self._bins,
                               key=lambda b: b.efficiency()[RType.CPU_core])
        idx = self._bins.index(self._currentBin)
        del self._bins[idx]
        self._currentBin.close()
        return True

    def head(self):
        if self._currentBin is None:
            if not self._loadNextBin():
                return None
        task = self._currentBin.getNext()
        if task is None:
            if not self._loadNextBin():
                return None
            task = self._currentBin.getNext()
        return task.vm

    def popFront(self):
        task = self._currentBin.popNext()
        if self._listener is not None:
            self._listener.addExpectation(what=NType.JobFinish, job=task.job)
        return task.vm

    @staticmethod
    def checkFitting(bucket, task):
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
        task = Task(vm, host_freqs=self._host_freqs, gpus_nCC=self._gpu_min_nCC)
        length = task.length
        bestBucket = None
        bestScore = {}
        for key in self._maxDims.keys():
            bestScore[key] = -INF
        for bucket in self._bins:
            score = self.checkFitting(bucket, task)
            if score is None:
                continue
            if score[RType.CPU_core] > bestScore[RType.CPU_core]:
                bestBucket = bucket
                bestScore = score
        if bestBucket is None:
            self._bins += [self.BinClass(self._maxDims)]
            bestBucket = self._bins[-1]
        added = bestBucket.add(task)
        if not added:
            raise Exception(f"{vm.name} cannot be fit into any bucket")
        return task.length

