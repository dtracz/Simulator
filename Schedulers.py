from abc import ABCMeta, abstractmethod
from toolkit import *
from Simulator import *
from Events import *
from Machine import *



class JobSchedulerSimple(NotificationListener):
    def __init__(self, machine, autofree=False):
        self._machine = machine
        self._jobQueue = []
        self._autofree = autofree and isinstance(machine, VirtualMachine)
        self._finished = False

    def isFittable(self, job):
        for name, value in job.resourceRequest.items():
            if name not in self._machine.resourceRequest.keys():
                return False
            if value != float('inf') and \
               self._machine.resourceRequest[name] < value:
                return False
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
        for name, value in job.resourceRequest.items():
            if value != float('inf') and \
               self._machine._resources[name].value < value:
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
        if self._autoFreeHost():
            return
        if isinstance(event, JobFinish) and \
           event.job.machine == self._machine:
            self._tryRunNext()
        if isinstance(event, JobStart) and \
           event.job.machine == self._machine:
            self._tryRunNext()
        if isinstance(event, VMStart) and \
           event.vm == self._machine:
            self._tryRunNext()



