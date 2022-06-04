from Simulator import *


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
        notif = Notification(NType.JobFinish, job=self.job)
        Simulator.getInstance().emit(notif)



class JobStart(Event):
    """
    Job start event.
    Allocates resources for job and schedules it's finish.
    """
    def __init__(self, job, priority=0, f=lambda: None):
        super().__init__(f, f"JobStart_{job.name}", priority)
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
        notif = Notification(NType.JobStart, job=self.job)
        Simulator.getInstance().emit(notif)



class TryJobStart(JobStart):
    def proceed(self):
        isAllocated = self.job.machine.allocate(self.job, noexcept=True)
        self._f(isAllocated)
        if not isAllocated:
            return
        self._time = Simulator.getInstance().time
        self.scheduleFinish()
        notif = Notification(NType.JobStart, job=self.job)
        Simulator.getInstance().emit(notif)



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
        notif = Notification(NType.JobRecalculate, job=self.job)
        Simulator.getInstance().emit(notif)



class VMStart(Event):
    def __init__(self, host, vm, priority=10):
        super().__init__(lambda: None, f"VMStart_{vm.name}", priority)
        self.host = host
        self.vm = vm

    def proceed(self):
        self.host.allocate(self.vm)
        notif = Notification(NType.VMStart, host=self.host, vm=self.vm)
        Simulator.getInstance().emit(notif)



class VMEnd(Event):
    def __init__(self, host, vm, priority=20):
        super().__init__(lambda: None, f"VMEnd_{vm.name}", priority)
        self.host = host
        self.vm = vm

    def proceed(self):
        self.host.free(self.vm)
        notif = Notification(NType.VMEnd, host=self.host, vm=self.vm)
        Simulator.getInstance().emit(notif)


