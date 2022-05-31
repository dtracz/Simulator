from sortedcontainers import SortedDict
from Simulator import *
from Resource import *


class Job(ResourcesHolder):
    """
    Job structure. Contains all information about job
    and provides mothods of it's procedure and maintenance.
    """
    _noCreated = 0

    def __init__(self, operations, resourceRequest, machine=None, name=None):
        super().__init__(resourceRequest)
        self._index = Job._noCreated
        if (name is None):
            name = f"Job_{self._index}"
        self.name = name
        self.machine = machine
        self.operations = operations
        self.operationsLeft = operations
        self.predictedFinish = None
        self._updates = [] # [(time, speed)]
        Job._noCreated += 1

    def asignMachine(self, machine):
        self.machine = machine

    def allocateResources(self):
        if self.machine == None:
            raise RuntimeError("No machine selected")
        self.machine.allocate(self)

    def freeResources(self):
        if self.machine == None:
            raise RuntimeError("No machine selected")
        self.machine.free(self)

    def getCurrentSpeed(self):
        totalFrequency = 0
        for resource in self.obtainedRes:
            if resource.rtype == Resource.Type.CPU_core:
                totalFrequency += resource.value
        return totalFrequency

    def calculateExecTime(self):
        totalFrequency = self.getCurrentSpeed()
        if totalFrequency == 0:
            raise Exception(f"No cores provided for {self.name}")
        return self.operationsLeft / totalFrequency

    def registerProgress(self, time="now"):
        if len(self._updates) == 0:
            raise Exception("Job has not yet begun")
        if time == "now":
            time = Simulator.getInstance().time
        startTime = self._updates[-1][0]
        speed = self._updates[-1][1]
        opsDone = (time - startTime) * speed
        self.operationsLeft -= min(self.operationsLeft, opsDone)
        if self.operationsLeft < EPS:
            self.operationsLeft = 0
        return opsDone

    def update(self):
        time = Simulator.getInstance().time
        if len(self._updates) != 0:
            assert(time >= self._updates[-1][0])
        currentSpeed = self.getCurrentSpeed()
        self._updates += [(time, currentSpeed)]

