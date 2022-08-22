#  from multiset import Multiset
from toolkit import *
from Simulator import *
from Events import *
from Resource import *
from Machine import *
from Job import *


class Task:
    '''
    Assumptions:
    1. All GPUs of the host machine are the same.
    2. All CPU cores of the host machine are the same.
    3. Tasks fully utilizes resource they requested.
    '''
    def __init__(self, vm, host_freqs={RType.CPU_core: 1}, gpus_nCC=None):
        self.vm = vm
        assert vm._jobScheduler is not None
        assert len(vm._jobScheduler._jobQueue) == 1
        self.job = vm._jobScheduler._jobQueue[0]
        self.dims = {}
        rams = list(filter(lambda rv: rv[0] == RType.RAM,
                           vm.maxResources))
        assert len(rams) == 1
        self.dims[rams[0][0]] = rams[0][1]
        cores = list(filter(lambda rv: rv[0] == RType.CPU_core,
                            vm.maxResources))
        assert len(cores) > 0
        self.dims[cores[0][0]] = len(cores)
        gpus = list(filter(lambda rv: rv[0] == RType.GPU,
                           vm.maxResources))
        if len(gpus) > 0:
            if gpus_nCC is None:
                raise KeyError(f"Number of Cuda Cores not provided for task")
            if RType.GPU not in host_freqs.keys():
                raise KeyError(f"GPU frequency not provided for task")
            self.dims[gpus[0][0]] = len(gpus)
            self.gpus_nCC = gpus_nCC
        self.host_freqs = host_freqs
        self.startpoint = None

    def reduceCores(self, n=-1):
        noCores = self.dims[RType.CPU_core]
        if n <= 0:
            n += noCores
        if n <= 0:
            raise Exception("Task has to have at leat 1 core assigned")
        if n < noCores:
            self.dims[RType.CPU_core] = n
        return noCores

    def restoreCores(self, n=INF):
        maxCores = len(list(filter(lambda r: r.rtype == RType.CPU_core,
                                  self.job.resourceRequest)))
        currentCores = self.dims[RType.CPU_core]
        self.dims[RType.CPU_core] = min(maxCores, currentCores + n)
        return self.dims[RType.CPU_core]

    @property
    def length(self):
        opss = []
        for rtype, ops in self.job.operations.items():
            noCores = self.dims[rtype] * self.host_freqs[rtype]
            if rtype is RType.GPU:
                noCores *= self.gpus_nCC
            opss += [ops / noCores]
        return max(opss)

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
        self.allTasks = Multiset()

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
        if len(self.timepoints()) > 0:
            assert len(self._dict[self.timepoints()[-1]]) == 0
        if time not in self._dict.keys():
            self._dict[time] = set()
            self._completePoint(time)
        if time + task.length not in self._dict.keys():
            self._dict[time + task.length] = set()
            self._completePoint(time + task.length)
        for t in self._dict.irange(time, time + task.length, (True, False)):
            self._dict[t].add(task)
        self.allTasks.add(task)
        task.startpoint = time
        if len(self.timepoints()) > 0:
            assert len(self._dict[self.timepoints()[-1]]) == 0

    def remove(self, task):
        if len(self.timepoints()) > 0:
            assert len(self._dict[self.timepoints()[-1]]) == 0
        self.allTasks.remove(task, 1)
        for t in self._dict.irange(task.startpoint, task.endpoint, (True, False)):
            self._dict[t].remove(task)
        self._clearPoint(task.startpoint)
        self._clearPoint(task.endpoint)
        task.startpoint = None
        if len(self.timepoints()) > 0:
            assert len(self._dict[self.timepoints()[-1]]) == 0

    def __getitem__(self, time):
        assert time >= 0
        if time in self._dict.keys():
            return self._dict[time]
        time_ = self._dict.first_key_lower(time)
        return self._dict[time_]

    def timepoints(self):
        return self._dict.keys()

    def __iter__(self):
        tasksSet = set()
        tasksList = []
        for tasksAt in self._dict.values():
            tasksAt = list(tasksAt)
            tasksAt.sort(key=lambda t: t.job._index)
            for task in tasksAt:
                if task not in tasksSet:
                    tasksList += [task]
                    tasksSet.add(task)
        return iter(tasksList)

    def copy(self):
        tl = Timeline()
        for time, tasks in self._dict.items():
            tl._dict[time] = tasks.copy()
        tl.allTasks = self.allTasks.copy()
        return tl

