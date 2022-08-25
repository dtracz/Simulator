from Simulator import *


class JobFinish(Event):
    """
    Job finish event.
    Releases resources at the end of the job.
    Usually scheduled automatically.
    """
    def __init__(self, job, host, priority=80):
        super().__init__(lambda: None, f"JobFinish_{job.name}", priority)
        self.job = job
        self.host = host
        self._time = None

    def proceed(self):
        self._time = Simulator.getInstance().time
        self.job.registerProgress()
        self.host.free(self.job)
        notif = Notification(NType.JobFinish, job=self.job, host=self.host)
        Simulator.getInstance().emit(notif)



class JobStart(Event):
    """
    Job start event.
    Allocates resources for job and schedules it's finish.
    """
    def __init__(self, job, host, priority=0, f=lambda: None):
        super().__init__(f, f"JobStart_{job.name}", priority)
        self.job = job
        self.host = host
        self._time = None

    def scheduleFinish(self):
        execTime = self.job.calculateExecTime()
        endTime = self._time + execTime
        jobFinish = JobFinish(self.job, self.host)
        Simulator.getInstance().addEvent(endTime, jobFinish)
        self.job.predictedFinish = jobFinish
        self.job.update()

    def proceed(self):
        self._time = Simulator.getInstance().time
        self.host.allocate(self.job)
        self.scheduleFinish()
        notif = Notification(NType.JobStart, job=self.job, host=self.host)
        Simulator.getInstance().emit(notif)



class TryJobStart(JobStart):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = f"JobTryStart_{self.job.name}"

    def proceed(self):
        isAllocated = self.host.allocate(self.job, noexcept=True)
        self._f(isAllocated)
        if not isAllocated:
            return
        self._time = Simulator.getInstance().time
        self.scheduleFinish()
        notif = Notification(NType.JobStart, job=self.job, host=self.host)
        Simulator.getInstance().emit(notif)



class JobRecalculate(Event):
    """
    Job recalculation. Needs to be proceed when
    some resources of already running job change.
    """
    def __init__(self, job, host, priority=100):
        super().__init__(lambda: None, f"JobRecalculate_{job.name}", priority)
        self.job = job
        self.host = host
        self._time = None

    def scheduleFinish(self):
        self._time = Simulator.getInstance().time
        execTime = self.job.calculateExecTime()
        endTime = self._time + execTime
        jobFinish = JobFinish(self.job, self.host)
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
        notif = Notification(NType.JobRecalculate, job=self.job, host=self.host)
        Simulator.getInstance().emit(notif)



class VMStart(Event):
    def __init__(self, host, vm, priority=10):
        super().__init__(lambda: None, f"VMStart_{vm.name}", priority)
        self.host = host
        self.vm = vm

    def proceed(self):
        self.host.allocate(self.vm)
        notif = Notification(NType.VMStart, vm=self.vm, host=self.host)
        Simulator.getInstance().emit(notif)



class VMEnd(Event):
    def __init__(self, host, vm, priority=20):
        super().__init__(lambda: None, f"VMEnd_{vm.name}", priority)
        self.host = host
        self.vm = vm

    def proceed(self):
        self.host.free(self.vm)
        notif = Notification(NType.VMEnd, vm=self.vm, host=self.host)
        Simulator.getInstance().emit(notif)



class VMShedule(Event):
    def __init__(self, target, vm, priority=0):
        assert hasattr(target, 'scheduleVM')
        super().__init__(lambda: None, f"VMShedule{vm.name}", priority)
        self.target = target
        self.vm = vm

    def proceed(self):
        self.target.scheduleVM(self.vm)
        notif = Notification(NType.Other, message='VMSchedule',
                             vm=self.vm, target=self.target)
        Simulator.getInstance().emit(notif)

