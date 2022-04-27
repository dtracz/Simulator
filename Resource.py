from enum import Enum
from Events import *


class Resource:
    """
    Bisic resource class. Represents it's type and values
    (max possible and current), and provides information about
    all jobs that re using this resource at the moment.
    Provides methonds of allocation and freeing resource.
    """
    class Type(Enum):
        CPU_core = 1
        RAM = 2

        def __lt__(self, other):
            return self.value < other.value


    def __init__(self, rtype, value):
        self.rtype = rtype 
        self.value = value
        self.maxValue = value
        self.jobsUsing = set()

    def withold(self, value):
        """
        value -- amount of resource to withold.
        Reduces current value of resource by requested value.
        Returns new Resource representing obtained resource.
        """
        if value == float('inf'):
            value = self.value
        if value > self.value:
            raise RuntimeError(f"Requested {value} out of {self.value} avaliable")
        self.value -= value
        return Resource(self.rtype, value)

    def release(self, value):
        if value == float('inf'):
            self.value = self.maxValue
        elif self.value + value <= self.maxValue:
            self.value += value
        else:
            raise RuntimeError("Resource overflow after release")

    def allocate(self, requestedValue, job):
        resource = self.withold(requestedValue)
        job.obtainedRes[id(self)] = resource
        self.jobsUsing.add(job)

    def free(self, job):
        resource = job.obtainedRes[id(self)]
        self.release(resource.value)
        del job.obtainedRes[id(self)]
        self.jobsUsing.remove(job)

    def __lt__(self, other):
        if self.rtype != other.rtype:
            return self.rtype < other.rtype
        return self.value < other.value

    def __hash__(self):
        return id(self)



class SharedResource(Resource):
    """
    Resource that is automatically spread among all jobs
    that are using it at the moment.
    If n jobs are using SharedResource, all of them benefits equally
    from 1/n of it's value. If job starts of finishes to use SharedResource,
    all other using it at the moment are recalculated.
    """
    def withold(self, value):
        if value != self.maxValue and value != float('inf'):
            raise Exception("SharedResource cannot be partially obtained")
        return self

    def release(self, value):
        if value != self.maxValue and value != float('inf'):
            raise Exception("SharedResource cannot be partially freed")

    def recalculateJobs(self, excluded=[]):
        now = Simulator.getInstance().time
        for job in self.jobsUsing:
            if job in excluded:
                continue
            jobRecalculate = JobRecalculate(job)
            Simulator.getInstance().addEvent(now, jobRecalculate)

    def allocate(self, requestedValue, job):
        if requestedValue != float('inf'):
            return super().allocate(requestedValue, job)
        self.jobsUsing.add(job)
        self.value = self.maxValue / len(self.jobsUsing)
        job.obtainedRes[id(self)] = self
        self.recalculateJobs([job])

    def free(self, job):
        resource = job.obtainedRes[id(self)]
        self.jobsUsing.remove(job)
        if len(self.jobsUsing) > 0:
            self.value = self.maxValue / len(self.jobsUsing)
        else:
            self.value = self.maxValue
        del job.obtainedRes[id(self)]
        self.recalculateJobs([job])

