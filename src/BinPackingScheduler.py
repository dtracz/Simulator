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
        self.startpoint = None

    @property
    def length(self):
        ops = self.job.operations
        noCores = len(list(filter(lambda r: r.rtype == Resource.Type.CPU_core,
                                  self.vm.resourceRequest)))
        return ops / noCores



class Timeline:
    def __init__(self):
        self._dict = Map()

    def _completePoint(self, time):
        assert time in self.timepoints()
        if self._dict.peekitem(0)[0] < time:
            time_ = self._dict.first_key_lower(time)
            tasks = self._dict[time_]
            for task in tasks:
                if task.startpoint + task.length > time:
                    self._dict[time].add(task)

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

    def remove(self, time, task):
        prevEmpty = True
        if self._dict.peekitem(0)[0] < time:
            time_ = self._dict.first_key_lower(time)
            prevEmpty = len(self._dict[time_]) == 0
        for t in self._dict.irange(time, time + task.length, (True, False)):
            self._dict[t].remove(task)
            if len(self._dict[t]) == 0:
                if prevEmpty:
                    del self._dict[t]
                else:
                    prevEmpty = True
            else:
                prevEmpty = False
        time_ = time + task.length
        assert time_ in self._dict.keys()
        assert task not in self._dict[time]
        if prevEmpty and len(self._dict[time_]) == 0:
            del self._dict[time_]
        task.startpoint = None

    def __getitem__(self, time):
        assert time >= 0
        if time in self._dict.keys():
            return self._dict[time]
        time_ = self._dict.first_key_lower(time)
        return self._dict[time_]

    def timepoints(self):
        return self._dict.keys()

