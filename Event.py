from Simulator import Simulator

class Event:
    """
    Simple 0-argument function wrapper.
    Basic Event executed by Simulator.
    """
    _noCreated = 0

    def __init__(self, f, name=None, priority=0):
        self._f = f
        self._index = Event._noCreated
        if (name is None):
            name = f"Event_{self._index}"
        self.name = name
        self._priority = priority
        Event._noCreated += 1

    def proceed(self):
        self._f()

    def __lt__(self, other):
        if self._priority != other._priority:
            return self._priority > other._priority
        return self._index < other._index

    def __eq__(self, other):
        return self._index == other._index

    def __hash__(self):
        return self._index
        


class JobFinish(Event):
    """
    Job finish event.
    Releases resources at the end of the job.
    Usually scheduled automatically.
    """
    def __init__(self, job, priority=80):
        super().__init__(lambda: None, f"JobFinish_{job.name}", priority)
        self._job = job
        self._time = None

    def proceed(self):
        self._time = Simulator.getInstance().time
        self._job.freeResources()



class JobStart(Event):
    """
    Job start event.
    Allocates resources for job and schedules it's finish.
    """
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
    """
    Job recalculation. Needs to be proceed when
    some resources of already running job change.
    """
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

        
