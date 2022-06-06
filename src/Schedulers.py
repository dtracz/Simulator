from abc import ABCMeta, abstractmethod
from toolkit import *
from Simulator import *
from Events import *
from Machine import *



class VMSchedulerSimple(NotificationListener):
    def __init__(self, machine):
        self._machine = machine
        self._vmQueue = []
        self._suspended = False

    def head(self):
        if len(self._vmQueue) == 0:
            return None
        return self._vmQueue[0]

    def popFront(self):
        return self._vmQueue.pop(0)

    def _tryAllocate(self):
        if self.head() is None:
            return False
        vm = self.head()
        def f():
            self._suspended = False
            isAllocated = self._machine.allocate(vm, noexcept=True)
            if not isAllocated:
                return
            self.popFront()
            notif = Notification(NType.VMStart, host=self._machine, vm=vm)
            Simulator.getInstance().emit(notif)
        Simulator.getInstance().addEvent(NOW(), Event(f, priority=10))
        self._suspended = True
        return True

    def schedule(self, vm):
        if not self._machine.isFittable(vm):
            raise Exception(f"{vm.name} can never be allocated"
                            f" on {self._machine.name}")
        self._vmQueue += [vm]

    def notify(self, notif):
        if self._suspended:
            return
        if notif.what == NType.VMStart and \
           notif.host == self._machine:
            self._tryAllocate()
        if notif.what == NType.VMEnd and \
           notif.host == self._machine:
            self._tryAllocate()
        if notif.what == NType.Other and \
           notif.message == "SimulationStart":
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
        self._suspended = False

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
        def f(isAllocated):
            if isAllocated:
                self._jobQueue.pop(0)
            self._suspended = False
        event = TryJobStart(job, self._machine, f=f)
        Simulator.getInstance().addEvent(NOW(), event)
        self._suspended = True
        return True

    def schedule(self, job):
        if self._finished:
            raise Exception(f"Scheduler out of operation")
        if not self._machine.isFittable(job):
            raise Exception(f"{job.name} can never be allocated on"
                            f"{self._machine.name}")
        self._jobQueue += [job]

    def notify(self, notif):
        if self._suspended:
            return
        if notif.what == NType.JobFinish and \
           notif.host == self._machine:
            if self._autoFreeHost():
                return
            self._tryRunNext()
        if notif.what == NType.JobStart and \
           notif.host == self._machine:
            self._tryRunNext()
        if notif.what == NType.VMStart and \
           notif.vm == self._machine:
            self._tryRunNext()
        if notif.what == NType.Other and \
           notif.message == "SimulationStart":
            self._tryRunNext()

