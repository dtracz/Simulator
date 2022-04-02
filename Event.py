from Simulator import Simulator

class Event:
    _index = 0

    def __init__(self, f, name=None, priority=0):
        self._f = f
        if (name is None):
            name = f"Event_{Event._index}"
        self.name = name
        self._priority = priority
        Event._index += 1

    def proceed(self):
        self._f()

    def __lt__(self, other):
        if self._priority != other._priority:
            return self._priority > other._priority
        return self._index < other._index
        


class JobFinish(Event):
    def __init__(self, job, priority=80):
        super().__init__(lambda: None, f"JobFinish_{job.name}", priority)
        self._job = job
        self._time = None

    def proceed(self):
        self._time = Simulator.getInstance().time
        self._job.freeResources()



class JobStart(Event):
    def __init__(self, job, priority=0):
        super().__init__(lambda: None, f"JobStart_{job.name}", priority)
        self._job = job
        self._time = None

    def scheduleFinish(self):
        self._time = Simulator.getInstance().time
        execTime = self._job.calculateExecTime()
        endTime = self._time + execTime
        jobFinish = JobFinish(self._job)
        Simulator.getInstance().addEvent(endTime, jobFinish)
        self._job.predictedFinish = jobFinish
        self._job.update()

    def proceed(self):
        self._job.allocateResources()
        self.scheduleFinish()



class JobRecalculate(JobStart):
    def __init__(self, job, priority=100):
        super().__init__(job, priority)
        self.name = f"JobRecalculate_{job.name}"

    def deletePrevFinish(self):
        jobFinish = self._job.predictedFinish
        if jobFinish is None:
            return
        Simulator.getInstance().removeEvent(jobFinish)

    def proceed(self):
        self._job.registerProgress()
        self.deletePrevFinish()
        self.scheduleFinish()

        
