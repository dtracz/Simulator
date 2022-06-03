from abc import ABCMeta, abstractmethod
from toolkit import *
from Simulator import *
from Events import *
from Machine import *



class VMSchedulerSimple(NotificationListener):
    def __init__(self, machine):
        self._machine = machine
        self._vmQueue = []

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
        if not self._machine.isFittable(vm):
            raise Exception(f"{vm.name} can never be allocated on {self._machine.name}")
        self._vmQueue += [vm]

    def notify(self, event):
        if isinstance(event, VMStart) and \
           event.host == self._machine:
            self._tryAllocate()
        if isinstance(event, VMEnd) and \
           event.host == self._machine:
            self._tryAllocate()
        if event.name == "SimulationStart":
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
            if scheduler._machine.isFittable(vm):
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

    def _autoFreeHost(self):
        if not self._finished and self._autofree and \
           len(self._jobQueue) == 0 and \
           len(self._machine.jobsUsing) == 0 and \
           len(self._machine.vmsUsing) == 0:
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
        for req in job.resourceRequest:
            try:
                res = self._machine.getBestFitting(req.rtype, req.value, excluded)
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
        if not self._machine.isFittable(job):
            raise Exception(f"{job.name} can never be allocated on {self._machine.name}")
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
        if event.name == "SimulationStart":
            self._tryRunNext()



