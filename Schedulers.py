from abc import ABCMeta, abstractmethod
from toolkit import *
from Simulator import *
from Events import *
from Machine import *



class VMSchedulerSimple(NotificationListener):
    def __init__(self, machine):
        self._machine = machine
        self._vmQueue = []

    def isFittable(self, vm):
        used = []
        for req in vm.resourceRequest:
            f = lambda r: r.rtype == req.rtype and r not in used
            avaliableRes = list(filter(f, self._machine._resources.getAll(req.rtype)))
            if len(avaliableRes) == 0:
                return False
            if req.value == float('inf'):
                used += [min(avaliableRes, key=lambda r: r.value)]
            else:
                avaliableRes = list(filter(lambda r: r.value >= req.value, avaliableRes))
                if len(avaliableRes) == 0:
                    return False
                used += [min(avaliableRes, key=lambda r: r.value)]
        return True

    def _tryAllocate(self):
        if len(self._vmQueue) == 0:
            return False
        vm = self._vmQueue[0]
        excluded = []
        for req in vm.resourceRequest:
            try:
                res = self._machine.getBestFitting(req.rtype, req.value, excluded)
                excluded += [res]
            except:
                return False
        now = Simulator.getInstance().time
        event = VMStart(self._machine, vm)
        Simulator.getInstance().addEvent(now, event)
        self._vmQueue.pop(0)
        return True

    def schedule(self, vm):
        if not self.isFittable(vm):
            raise Exception(f"{vm.name} can never be allocated on {self._machine.name}")
        self._vmQueue += [vm]

    def notify(self, event):
        if event.name == "SimulationStart":
            self._tryAllocate()
        if isinstance(event, VMStart) and \
           event.host == self._machine:
            self._tryAllocate()
        if isinstance(event, VMEnd) and \
           event.host == self._machine:
            self._tryAllocate()



class VMPlacmentPolicySimple:
    def __init__(self, machines):
        self._schedulers = {}
        self._noVMs = MultiDictRevDict()
        for machine in machines:
            self._schedulers[machine] = VMSchedulerSimple(machine)
            self._noVMs.add(0, machine)

    def placeVM(self, vm):
        tried = []
        while len(self._noVMs) > 0:
            noVMs, machine = self._noVMs.popitem()
            scheduler = self._schedulers[machine]
            if scheduler.isFittable(vm):
                scheduler.schedule(vm)
                self._noVMs.add(noVMs+1, machine)
                break
            tried += [(noVMs, machine)]
        exausted = len(self._noVMs) == 0
        for noVMs, machine in tried:
            self._noVMs.add(noVMs, machine)
        if exausted:
            raise Exception(f"Non of the known machines is suitable for {vm.name}")



class JobSchedulerSimple(NotificationListener):
    def __init__(self, machine, autofree=False):
        self._machine = machine
        self._jobQueue = []
        self._autofree = autofree and isinstance(machine, VirtualMachine)
        self._finished = False

    def isFittable(self, job):
        used = []
        for req in job.resourceRequest:
            req = ResourceRequest(req[0], req[1])
            f = lambda r: r.rtype == req.rtype and r not in used
            avaliableRes = list(filter(f, self._machine.resourceRequest))
            if len(avaliableRes) == 0:
                return False
            if req.value == float('inf'):
                used += [min(avaliableRes, key=lambda r: r.value)]
            else:
                avaliableRes = list(filter(lambda r: r.value >= req.value, avaliableRes))
                if len(avaliableRes) == 0:
                    return False
                used += [min(avaliableRes, key=lambda r: r.value)]
        return True

    def _autoFreeHost(self):
        if not self._finished and self._autofree and \
           len(self._jobQueue) == 0 and \
           len(self._machine.jobsRunning) == 0:
            now = Simulator.getInstance().time
            event = VMEnd(self._machine.host, self._machine)
            Simulator.getInstance().addEvent(now, event)
            self._finished = True
            return True
        return False

    def _tryRunNext(self):
        if len(self._jobQueue) == 0:
            return False
        job = self._jobQueue[0]
        excluded = []
        for rtype, value in job.resourceRequest:
            try:
                res = self._machine.getBestFitting(rtype, value, excluded)
                excluded += [res]
            except:
                return False
        now = Simulator.getInstance().time
        Simulator.getInstance().addEvent(now, JobStart(job))
        self._jobQueue.pop(0)
        return True

    def schedule(self, job):
        if self._finished:
            raise Exception(f"Scheduler out of operation")
        if not self.isFittable(job):
            raise Exception(f"{vm.name} can never be allocated on {self._machine.name}")
        self._jobQueue += [job]

    def notify(self, event):
        if isinstance(event, JobFinish) and \
           event.job.machine == self._machine:
            if self._autoFreeHost():
                return
            self._tryRunNext()
        if isinstance(event, JobStart) and \
           event.job.machine == self._machine:
            self._tryRunNext()
        if isinstance(event, VMStart) and \
           event.vm == self._machine:
            self._tryRunNext()



