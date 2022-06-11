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


    def __init__(self, machine, BinClass=SimpleBin):
        super().__init__(machine)
        self.BinClass = BinClass
        self._maxDims = {}
        rams = list(filter(lambda r: r.rtype == RType.RAM,
                           machine.resources))
        assert len(rams) == 1
        self._maxDims[rams[0].rtype] = rams[0].value
        cores = list(filter(lambda r: r.rtype == RType.CPU_core,
                            machine.resources))
        assert len(cores) > 0
        self._maxDims[cores[0].rtype] = len(cores)
        self._bins = []
        self._currentBin = None

    @property
    def vmsLeft(self):
        n = 0
        for bucket in self._bins:
            n += len(bucket._tasks)
        return n

    def _loadNextBin(self):
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
            if score[RType.CPU_core] > bestScore[RType.CPU_core]:
                bestBucket = bucket
                bestScore = score
        if bestBucket is None:
            self._bins += [self.BinClass(self._maxDims)]
            bestBucket = self._bins[-1]
        if not bestBucket.add(task):
            raise Exception(f"{vm.name} cannot be fit into any bucket")

