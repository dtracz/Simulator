from abc import ABCMeta, abstractmethod
from toolkit import *
from Simulator import *
from Event import *
from Machine import *



class JobSchedulerSimple(NotificationListener):
    def __init__(self, machine):
        self._machine = machine
        self._jobQueue = []

    def isFittable(self, job):
        for name, value in job.resourceRequest.items():
            if name not in self._machine.resourceRequest.keys():
                return False
            if value != float('inf') and \
               self._machine.resourceRequest[name] < value:
                return False
        return True

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
        if not self.isFittable(job):
            raise Exception(f"{vm.name} can never be allocated on {self._machine.name}")
        self._jobQueue += [job]
        
    def notify(self, event):
        if isinstance(event, JobFinish) and \
           event.job.machine == self._machine:
            self._tryRunNext()
        if isinstance(event, JobStart) and \
           event.job.machine == self._machine:
            self._tryRunNext()
        if isinstance(event, VMStart) and \
           event.vm == self._machine:
            self._tryRunNext()



