from Simulator import Simulator

class Event:
    _index = 0

    def __init__(self, f, name=None, priority=0):
        self._f = f
        if (name is None):
            name = f"Event_{Event._index}"
        self._name = name
        self._priority = priority
        Event._index += 1

    def proceed(self):
        self._f()

    def __lt__(self, other):
        if self._priority != other._priority:
            return self._priority > other._priority
        return self._index < other._index
        


class JobFinish(Event):
    def __init__(self, job):
        super().__init__(lambda: None, f"JobStart_{job.name}")
        self._job = job
        self._time = None

    def proceed(self):
        self._time = Simulator.getInstance().time
        self._job.freeResources()
        #  for listener in Simulator.getInstance().listeners:
        #      listener.notify("Job done", self)



class JobReorganize(Event):
    pass



class JobStart(Event):
    def __init__(self, job):
        super().__init__(lambda: None, f"JobStart_{job.name}")
        self._job = job
        self._time = None

    def proceed(self):
        self._time = Simulator.getInstance().time
        self._job.allocateResources()
        execTime = self._job.calculateExecTime()
        endTime = self._time + execTime
        jobFinish = JobFinish(self._job)
        Simulator.getInstance().addEvent(endTime, jobFinish)
        #  for listener in Simulator.getInstance().listeners:
        #      listener.notify("Job done", self)

        
