from Simulator import Event, Simulator


class JobFinish(Event):
    """
    Job finish event.
    Releases resources at the end of the job.
    Usually scheduled automatically.
    """
    def __init__(self, job, priority=80):
        super().__init__(lambda: None, f"JobFinish_{job.name}", priority)
        self.job = job
        self._time = None

    def proceed(self):
        self._time = Simulator.getInstance().time
        self.job.registerProgress()
        self.job.freeResources()



class JobStart(Event):
    """
    Job start event.
    Allocates resources for job and schedules it's finish.
    """
    def __init__(self, job, priority=0):
        super().__init__(lambda: None, f"JobStart_{job.name}", priority)
        self.job = job
        self._time = None

    def scheduleFinish(self):
        execTime = self.job.calculateExecTime()
        endTime = self._time + execTime
        jobFinish = JobFinish(self.job)
        Simulator.getInstance().addEvent(endTime, jobFinish)
        self.job.predictedFinish = jobFinish
        self.job.update()

    def proceed(self):
        self._time = Simulator.getInstance().time
        self.job.allocateResources()
        self.scheduleFinish()



class JobRecalculate(Event):
    """
    Job recalculation. Needs to be proceed when
    some resources of already running job change.
    """
    def __init__(self, job, priority=100):
        super().__init__(lambda: None, f"JobRecalculate_{job.name}", priority)
        self.job = job
        self._time = None

    def scheduleFinish(self):
        self._time = Simulator.getInstance().time
        execTime = self.job.calculateExecTime()
        endTime = self._time + execTime
        jobFinish = JobFinish(self.job)
        Simulator.getInstance().addEvent(endTime, jobFinish)
        self.job.predictedFinish = jobFinish
        self.job.update()

    def deletePrevFinish(self):
        jobFinish = self.job.predictedFinish
        if jobFinish is None:
            return
        Simulator.getInstance().removeEvent(jobFinish)

    def proceed(self):
        self.job.registerProgress()
        self.deletePrevFinish()
        self.scheduleFinish()



class VMStart(Event):
    def __init__(self, host, vm, priority=10):
        super().__init__(lambda: None, f"VMStart_{vm.name}", priority)
        self.host = host
        self.vm = vm

    def proceed(self):
        self.host.allocate(self.vm)



class VMEnd(Event):
    def __init__(self, host, vm, priority=20):
        super().__init__(lambda: None, f"VMEnd_{vm.name}", priority)
        self.host = host
        self.vm = vm

    def proceed(self):
        self.host.free(self.vm)


