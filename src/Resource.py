from enum import Enum
from multiset import Multiset
from toolkit import *
from Events import *


class ResourceRequest:
    def __init__(self, rtype, value, shared=True):
        if value <= 0:
            raise Exception(f"Cannot request {rtype} of value {value}")
        if value != INF:
            shared = False
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
        self._users = {}
        #  self.vmsUsing = set()

    def addUser(self, user):
        if type(user) not in self._users.keys():
            self._users[type(user)] = Multiset()
        self._users[type(user)].add(user)

    def delUser(self, user):
        self._users[type(user)].remove(user, 1)
        if len(self._users[type(user)]) == 0:
            del self._users[type(user)]

    @property
    def jobsUsing(self):
        for key, val in self._users.items():
            if str(key) == "<class 'Job.Job'>":
                return set(val)
        return set()

    @property
    def vmsUsing(self):
        for key, val in self._users.items():
            if str(key) == "<class 'Machine.VirtualMachine'>":
                return set(val)
        return set()

    @property
    def avaliableValue(self):
        return self.tmpMaxValue

    @property
    def noDynamicUses(self):
        noDynamic = 0
        for job in self.jobsUsing:
            for req, x in job._resourceRequest.items():
                if x is None:
                    continue
                (srcRes, dstRes) = x
                noDynamic += dstRes is self
        return noDynamic

    def withold(self, req):
        resource = None
        if req.shared:
            self.value = self.tmpMaxValue / (self.noDynamicUses + 1)
            resource = self
        else:
            value = self.tmpMaxValue if req.value is INF else req.value
            if value > self.tmpMaxValue:
                raise RuntimeError(f"Requested {value} out of {self.value} avaliable")
            self.tmpMaxValue -= value
            self.value = self.tmpMaxValue / max(self.noDynamicUses, 1)
            resource = Resource(self.rtype, value)
        self.recalculateJobs()
        return resource

    def release(self, resource):
        if resource is self:
            self.value = self.tmpMaxValue / max(1, self.noDynamicUses - 1)
        elif self.tmpMaxValue + resource.value > self.maxValue + EPS:
            raise RuntimeError("Resource overflow after release")
        else:
            self.tmpMaxValue = min(self.tmpMaxValue + resource.value, self.maxValue)
            self.value = self.tmpMaxValue / max(self.noDynamicUses, 1)
        self.recalculateJobs()

    def recalculateJobs(self):
        now = Simulator.getInstance().time
        for job in self.jobsUsing:
            if job.host is None:
                continue
            if job.operationsLeft < EPS:
                continue
            jobRecalculate = JobRecalculate(job, job.host)
            Simulator.getInstance().addEvent(now, jobRecalculate)

    def __lt__(self, other):
        if self.rtype != other.rtype:
            return self.rtype < other.rtype
        return self.value < other.value

    def __hash__(self):
        return id(self)



class ResourcesHolder:
    def __init__(self, resourceRequest):
        self._resourceRequest = {} # {req: (srcRes, dstRes)}
        for req in resourceRequest:
            self._resourceRequest[req] = None
        self.host = None

    @property
    def isAllocated(self):
        noAllocated = len(list(filter(lambda x: x is not None,
                                      self._resourceRequest.values())))
        return noAllocated / len(self._resourceRequest)

    @property
    def resourceRequest(self):
        return list(self._resourceRequest.keys())

    @property
    def obtainedRes(self):
        resPairs = filter(lambda x: x is not None, self._resourceRequest.values())
        return [dstRes for srcRes, dstRes in resPairs]

    def setResources(self, reqResMap):
        for req, (srcRes, dstRes) in reqResMap.items():
            self._resourceRequest[req] = (srcRes, dstRes)
        noUnsatisfied = sum(res is None for res in self._resourceRequest.values())
        return noUnsatisfied

    def unsetResources(self):
        for req, (srcRes, dstRes) in self._resourceRequest.items():
            self._resourceRequest[req] = None
            yield (srcRes, dstRes)

