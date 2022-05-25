from enum import Enum
from toolkit import *
from Events import *


class ResourceRequest:
    def __init__(self, rtype, value, shared=False):
        if value <= 0:
            raise Exception(f"Cannot request {rtype} of value {value}")
        self.rtype = rtype
        self.value = value
        self.shared = shared



class Resource:
    """
    Bisic resource class. Represents it's type and values
    (max possible and current), and provides information about
    all jobs that re using this resource at the moment.
    Provides methonds of allocation and freeing resource.

    Resource could be automatically spread among all jobs
    that are using it at the moment. If n jobs are using
    Resource in this way, all of them benefits equally
    from 1/n of it's non witheld value.
    """
    class Type(Enum):
        CPU_core = 1
        RAM = 2

        def __lt__(self, other):
            return self.value < other.value


    def __init__(self, rtype, value):
        self.rtype = rtype 
        self.maxValue = value
        self.tmpMaxValue = value
        self.value = value
        self.jobsUsing = set()
        self.vmsUsing = set()

    @property
    def avaliableValue(self):
        return self.tmpMaxValue

    @property
    def noDynamicJobs(self):
        noDynamic = 0
        for job in self.jobsUsing:
            noDynamic += job.obtainedRes[id(self)] is self
        return noDynamic

    def allocate(self, requestedValue, job):
        resource = self.withold(requestedValue)
        job.obtainedRes[id(self)] = resource
        self.jobsUsing.add(job)

    def free(self, job):
        resource = job.obtainedRes[id(self)]
        self.release(resource)
        del job.obtainedRes[id(self)]
        self.jobsUsing.remove(job)

    @property
    def noDynamicJobs(self):
        noDynamic = 0
        for job in self.jobsUsing:
            noDynamic += job.obtainedRes[id(self)] is self
        return noDynamic

    def withold(self, value):
        resource = None
        if value == INF:
            self.value = self.tmpMaxValue / (self.noDynamicJobs + 1)
            resource = self
        elif value > self.tmpMaxValue: raise RuntimeError(f"Requested {value} out of {self.value} avaliable")
        else:
            self.tmpMaxValue -= value
            self.value = self.tmpMaxValue / max(self.noDynamicJobs, 1)
            resource = Resource(self.rtype, value)
        self.recalculateJobs()
        return resource

    def release(self, resource):
        if resource is self:
            self.value = self.tmpMaxValue / max(1, self.noDynamicJobs - 1)
        elif self.tmpMaxValue + resource.value > self.maxValue + EPS:
            raise RuntimeError("Resource overflow after release")
        else:
            self.tmpMaxValue = min(self.tmpMaxValue + resource.value, self.maxValue)
            self.value = self.tmpMaxValue / max(self.noDynamicJobs, 1)
        self.recalculateJobs()

    def recalculateJobs(self):
        now = Simulator.getInstance().time
        for job in self.jobsUsing:
            if job.operationsLeft == 0:
                continue
            jobRecalculate = JobRecalculate(job)
            Simulator.getInstance().addEvent(now, jobRecalculate)

    def __lt__(self, other):
        if self.rtype != other.rtype:
            return self.rtype < other.rtype
        return self.value < other.value

    def __hash__(self):
        return id(self)

