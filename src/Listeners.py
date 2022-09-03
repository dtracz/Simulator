from toolkit import INF
from Simulator import *



class EventInspector(NotificationListener):
    def __init__(self, expected=[]):
        self._expectations = []
        for kwargs in expected:
            self.addExpectation(**kwargs)

    def addExpectation(self, f=lambda n: True, **kwargs):
        def verification(notify):
            verify = f(notify)
            for name, value in kwargs.items():
                verify *= hasattr(notify, name) and \
                          getattr(notify, name) == value
            return verify
        self._expectations += [verification]

    def notify(self, notification):
        for i, f in enumerate(self._expectations):
            if f(notification):
                del self._expectations[i]
                return

    def allRegistered(self):
        return 0 == len(self._expectations)

    def verify(self):
        assert 0 == len(self._expectations)



class JobDelayMetric(NotificationListener):
    def __init__(self):
        self._jobs = {}

    def _add(self, job, time, i):
        if job not in self._jobs:
            self._jobs[job] = [0, None, job._priority]
        self._jobs[job][i] = time

    def notify(self, notif):
        if notif.what == NType.Other and \
           notif.message == "VMSchedule":
            for job in notif.vm._jobScheduler._jobQueue:
                self._add(job, NOW(), 0)
        if notif.what == NType.JobStart:
            self._add(notif.job, NOW(), 1)

    @staticmethod
    def _calculateCost(sched, start, f):
        raise NotImplementedError()

    def cost(self, acceptZeroSchedule=True):
        cost = 0
        for job, (sched, start, f) in self._jobs.items():
            assert start is not None
            assert acceptZeroSchedule or sched > 0
            assert start >= sched
            assert f(sched) > 0
            cost += self._calculateCost(sched, start, f)
        return cost / len(self._jobs)


class PriorityIncreaseMetric(JobDelayMetric):

    @staticmethod
    def _calculateCost(sched, start, f):
        return f(start) - f(sched)


class AUPMetric(JobDelayMetric):
    """
    Area Under Priority over time of waiting
    assuming linear increase
    """

    @staticmethod
    def _calculateCost(sched, start, f):
        t = start - sched
        p0 = f(sched)
        p1 = f(start)
        return t*(p1 + p0)/2



class EfficiencyCalculator(NotificationListener):
    def __init__(self):
        self._machines = {}
        self._running = {}

    def _add(self, machine, resources):
        if machine not in self._machines:
            self._machines[machine] = {}
        for rtype, value in resources.items():
            if rtype not in self._machines[machine]:
                self._machines[machine][rtype] = 0
            self._machines[machine][rtype] += value

    def _registerStart(self, job):
        resources = job.operationsLeft.copy()
        startTime = NOW()
        self._running[job] = (resources, startTime)

    def _registerFinish(self, job):
        resources, startTime = self._running[job]
        duration = NOW() - startTime
        for req in job.resourceRequest:
            if req.rtype not in resources:
                assert req.value is not INF
                resources[req.rtype] = req.value * duration
        del self._running[job]
        return resources

    def notify(self, notif):
        if notif.what == NType.JobStart:
            self._registerStart(notif.job)
        if notif.what == NType.JobFinish:
            resources = self._registerFinish(notif.job)
            host = notif.host
            while str(type(host)) != "<class 'Machine.Machine'>":
                host = host.host
            self._add(host, resources)

    def get(self):
        result = {}
        duration= NOW() - 0
        for machine, resources in self._machines.items():
            result[machine] = {}
            for rtype, value in resources.items():
                host_res = list(filter(lambda r: r.rtype == rtype,
                                       machine.resources))
                max_res = 0
                for res in host_res:
                    freq = 1 if res.freq is None else res.freq
                    max_res += duration * res.value * freq
                result[machine][rtype] = value/max_res
        return result

