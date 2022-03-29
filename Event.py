from Simulator import Simulator

class Event:
    _index = 0

    def __init__(self, f, name=None, priority=0):
        self._f = f
        if (name is None):
            name = f"Event {Event._index}"
        self._name = name
        self._priority = priority
        Event._index += 1

    def proceed(self):
        self._f()

    def __lt__(self, other):
        self._priority < other._priority
        


class Job(Event):

    def __init__(self, machine, operations, resources, name=None):
        super().__init__(lambda: None, name, 100)
        self._machine = machine
        self._operations = operations
        self._resources = resources

    def calculateExecTime(self):
        totalFrequency = 0
        for name, resource in self._resources.items():
            if name[:4] == "Core":
                totalFrequency += resource.value
        if totalFrequency == 0:
            raise Exception(f"No cores provided for {Event._name}")
        return self._operations / totalFrequency

    def _finish(self):
        for name, resource in self._resources.items():
            self._machine.release(name, resource)
        for listener in Simulator.getInstance().listeners:
            listener.notify("Job done", self)

    def proceed(self):
        for name, resource in self._resources.items():
            self._machine.withold(name, resource)
        time = Simulator.getInstance().time
        execTime = self.calculateExecTime()
        endTime = time + execTime
        finishEvent = Event(self._finish, "finished " + self._name)
        Simulator.getInstance().addEvent(endTime, finishEvent)

        
