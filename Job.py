from sortedcontainers import SortedDict


class Job:
    _index = 0

    def __init__(self, operations, resourceRequest=None, machine=None, name=None):
        if (name is None):
            name = f"Job_{Job._index}"
        self.name = name
        self.machine = machine
        self.operations = operations
        self.operationsLeft = operations
        self.requestedRes = resourceRequest
        self.obtainedRes = {}
        Job._index += 1
        self._lastUpdate = SortedDict()

    def asignMachine(self, machine):
        self.machine = machine

    def asignResources(self, resources):
        self.requestedRes = resources

    def allocateResources(self):
        if self.machine == None:
            raise RuntimeError("No machine selected")
        self.machine.allocate(self)

    def freeResources(self):
        if self.machine == None:
            raise RuntimeError("No machine selected")
        self.machine.free(self)

    def calculateExecTime(self):
        totalFrequency = 0
        for name, resource in self.obtainedRes.items():
            if name[:4] == "Core":
                totalFrequency += resource.value
        if totalFrequency == 0:
            raise Exception(f"No cores provided for {Event._name}")
        return self.operationsLeft / totalFrequency

