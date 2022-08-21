from sortedcontainers import SortedDict
from Simulator import *
from Resource import *


class Job(ResourcesHolder):
    """
    Job structure. Contains all information about job
    and provides mothods of it's procedure and maintenance.
    """
    _noCreated = 0

    def __init__(self, operations, resourceRequest, name=None):
        super().__init__(resourceRequest)
        self._index = Job._noCreated
        if (name is None):
            name = f"Job_{self._index}"
        assert type(name) is str
        self.name = name
        if type(operations) is not dict:
            operations = {RType.CPU_core: operations}
        self.operations = operations
        self.operationsLeft = operations.copy()
        self.predictedFinish = None
        self._updates = [] # [(time, speed)]
        Job._noCreated += 1

    def getCurrentSpeeds(self):
        totalFrequency = dict.fromkeys(self.operationsLeft.keys(), 0)
        for resource in self.obtainedRes:
            if resource.rtype in totalFrequency.keys():
                totalFrequency[resource.rtype] += resource.value
        return totalFrequency

    def calculateExecTimes(self):
        totalFrequency = self.getCurrentSpeeds()
        if any([v == 0 for v in totalFrequency.values()]):
            raise Exception(f"No cores provided for {self.name}")
        times = self.operationsLeft.copy()
        for rtype in times.keys():
            times[rtype] /= totalFrequency[rtype]
        return times

    def calculateExecTime(self):
        times = self.calculateExecTimes()
        return max(times.values())

    def registerProgress(self, time="now"):
        if len(self._updates) == 0:
            raise Exception("Job has not yet begun")
        if time == "now":
            time = Simulator.getInstance().time
        startTime = self._updates[-1][0]
        speeds = self._updates[-1][1]
        opsDone = {}
        for rtype, speed in speeds.items():
            opsDone[rtype] = (time - startTime) * speed
            opsDone[rtype] = min(self.operationsLeft[rtype], opsDone[rtype])
            self.operationsLeft[rtype] -= opsDone[rtype]
            if self.operationsLeft[rtype] < EPS:
                self.operationsLeft[rtype] = 0
        return opsDone

    def update(self):
        time = Simulator.getInstance().time
        if len(self._updates) != 0:
            assert(time >= self._updates[-1][0])
        currentSpeeds = self.getCurrentSpeeds()
        self._updates += [(time, currentSpeeds)]

