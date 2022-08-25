from multiset import Multiset
from Resource import *
from toolkit import *


class Infrastructure:
    """
    Infrastructure represents all hardware avaliable to run jobs.
    """

    def __init__(self, machines, getVMPlacementPolicy):
        self.machines = set(machines)
        self._knownVMs = set()
        self._vmPlacementPolicy = getVMPlacementPolicy(self.machines)

    def scheduleVM(self, vm):
        return self._vmPlacementPolicy.placeVM(vm)

    def scheduleJob(self, job, vm):
        if vm not in self._knownVMs:
            raise Exception(f"Requested job on unknown vm {vm.name}")
        vm.schedule(job)



class Machine:
    """
    Hardware machine, that holds resources and is able
    to run jobs or host virtual machines.
    """
    _noCreated = 0

    def __init__(self, name, resources,
                 getJobScheduler=lambda _: None,
                 getVMScheduler=lambda _: None):
        self._index = Machine._noCreated
        self.name = name
        self._resources = resources
        self._jobScheduler = getJobScheduler(self)
        self._vmScheduler = getVMScheduler(self)
        Machine._noCreated += 1
        self._users = {}

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
    def resources(self):
        return self._resources

    @property
    def maxResources(self):
        for res in self.resources:
            yield (res.rtype, res.maxValue)

    def getBestFitting(self, req):
        f = lambda r: r.rtype == req.rtype and r.value > 0
        allRes = list(filter(f, self.resources))
        if len(allRes) == 0:
            raise RuntimeError(f"Cannot find fitting {req.rtype}")
        if req.shared:
            f = lambda r: r.maxValue/(1 + r.noDynamicUses + len(r.vmsUsing))
            return max(allRes, key=f)
        elif req.value == INF:
            allRes = list(filter(lambda r: r.noDynamicUses == 0, allRes))
            if len(allRes) == 0:
                raise RuntimeError(f"Cannot find fitting {req.rtype}")
            return max(allRes, key=lambda r: r.value)
        else:
            allRes.sort(key=lambda r: r.value)
            for res in allRes:
                if res.value >= req.value:
                    return res
        raise RuntimeError(f"Cannot find fitting {req.rtype}")

    def allocate(self, resHolder, noexcept=False):
        if resHolder.isAllocated:
            raise Exception(f"{resHolder.name} is already allocated")
        #  reqResMap = {}
        try:
            for req in filter(lambda r: not r.shared, resHolder.resourceRequest):
                srcRes = self.getBestFitting(req)
                dstRes = srcRes.withold(req)
                assert dstRes.value > 0
                #  reqResMap[req] = (srcRes, dstRes)
                resHolder._resourceRequest[req] = (srcRes, dstRes)
                srcRes.addUser(resHolder)
            for req in filter(lambda r: r.shared, resHolder.resourceRequest):
                srcRes = self.getBestFitting(req)
                dstRes = srcRes.withold(req)
                assert dstRes.value > 0
                #  reqResMap[req] = (srcRes, dstRes)
                resHolder._resourceRequest[req] = (srcRes, dstRes)
                srcRes.addUser(resHolder)
        except RuntimeError:
            #  for srcRes, dstRes in reqResMap.values():
            for req, x in resHolder._resourceRequest.items():
                if x is None:
                    continue
                (srcRes, dstRes) = x
                srcRes.release(dstRes)
                srcRes.delUser(resHolder)
                resHolder._resourceRequest[req] = None
            if noexcept:
                return False
            raise RuntimeError(f"Resources allocation for {resHolder.name} "
                               f"on {self.name} failed")
        assert resHolder.isAllocated == 1
        #  job.setResources(reqResMap)
        self.addUser(resHolder)
        resHolder.setHost(self)
        return True

    def free(self, resHolder):
        if type(resHolder) not in self._users.keys() or \
           resHolder not in self._users[type(resHolder)]:
            raise Exception(f"{resHolder.name} is not allocated on this machine")
        for srcRes, dstRes in resHolder.unsetResources():
            assert srcRes in self.resources
            srcRes.release(dstRes)
            srcRes.delUser(resHolder)
        assert resHolder.isAllocated == 0
        self.delUser(resHolder)
        resHolder.unsetHost()

    def isFittable(self, resHolder):
        resources = {}
        for rtype, value in self.maxResources:
            if rtype not in resources.keys():
                resources[rtype] = Multiset()
            resources[rtype].add(value)
        for req in resHolder.resourceRequest:
            if req.rtype not in resources.keys():
                return False
        for req in filter(lambda r: not r.shared and r.value != INF,
                          resHolder.resourceRequest):
            avalRes = [v for v in resources[req.rtype] if v >= req.value]
            if len(avalRes) == 0:
                return False
            val = min(avalRes)
            resources[req.rtype].remove(val, 1)
            val -= req.value
            if val > 0:
                resources[req.rtype].add(val)
        for req in filter(lambda r: not r.shared and r.value == INF,
                          resHolder.resourceRequest):
            if len(resources[req.rtype]) == 0:
                return False
            val = min(resources[req.rtype])
            resources[req.rtype].remove(val, 1)
        for req in filter(lambda r: r.shared, resHolder.resourceRequest):
            if len(resources[req.rtype]) == 0:
                return False
        return True

    def scheduleJob(self, job):
        if self._jobScheduler is None:
            raise Exception(f"Machine {self.name} has no job scheduler")
        return self._jobScheduler.schedule(job)

    def scheduleVM(self, job):
        if self._vmScheduler is None:
            raise Exception(f"Machine {self.name} has no VM scheduler")
        return self._vmScheduler.schedule(job)

    def __lt__(self, other):
        return self._index < other._index

    def __eq__(self, other):
        if other is None:
            return False
        return self._index == other._index

    def __hash__(self):
        return self._index



class VirtualMachine(Machine, ResourcesHolder):
    """
    Machine, that could be allocated on other machines,
    ald use part of it's resources to run jobs.
    """
    def __init__(self, name, resourceRequest={},
                 getJobScheduler=lambda _: None,
                 getVMScheduler=lambda _: None):
        Machine.__init__(self, name, [], getJobScheduler, getVMScheduler)
        ResourcesHolder.__init__(self, resourceRequest)
        self._srcResMap = {}
        #  self.resourceRequest = resourceRequest

    @property
    def resources(self):
        return self.obtainedRes

    @property
    def maxResources(self):
        if len(self._resources) > 0:
            for res in self._resources:
                yield (res.rtype, res.maxValue)
        else:
            for req in self.resourceRequest:
                # TODO: request value of `inf`
                yield (req.rtype, req.value)

