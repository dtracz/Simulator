from Resource import *
from toolkit import *


class Infrastructure:
    """
    Infrastructure represents all hardware avaliable to run jobs.
    """
    __self = None

    def __init__(self, machines, getVMPlacementPolicy):
        if Infrastructure.__self != None:
            raise Exception("Creating another instance of Infrastructure is forbidden")
        self.machines = set(machines)
        self._knownVMs = set()
        self._vmPlacementPolicy = getVMPlacementPolicy(self.machines)
        Infrastructure.__self = self

    @staticmethod
    def getInstance(*args, **kwargs):
        if Infrastructure.__self == None:
            Infrastructure(*args, **kwargs)
        return Infrastructure.__self;

    def scheduleVM(self, vm):
        self._vmPlacementPolicy.placeVM(vm)

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
        self._resources = resources #self.makeResources(resources)
        self._hostedVMs = set()
        self.jobsRunning = set()
        self._jobScheduler = getJobScheduler(self)
        self._vmScheduler = getVMScheduler(self)
        Machine._noCreated += 1

    @property
    def maxResources(self):
        for res in self._resources:
            yield (res.rtype, res.maxValue)

    def getBestFitting(self, rtype, value, excluded=[]):
        allRes = list(filter(lambda r: r.rtype == rtype, self._resources))
        if value == INF:
            f = lambda r: r.maxValue/(1 + r.noDynamicUses + len(r.vmsUsing))
            return max(allRes, key=f)
        allRes.sort(key=lambda r: r.value)
        for res in allRes:
            if res.value >= value:
                return res
        raise RuntimeError(f"Cannot find fitting {rtype}")

    def allocate(self, job):
        #  reqResMap = {}
        try:
            for req in filter(lambda r: not r.shared, job.resourceRequest):
                srcRes = self.getBestFitting(req.rtype, req.value)
                dstRes = srcRes.withold(req)
                #  reqResMap[req] = (srcRes, dstRes)
                job._resourceRequest[req] = (srcRes, dstRes)
                srcRes.addUser(job)
            for req in filter(lambda r: r.shared, job.resourceRequest):
                srcRes = self.getBestFitting(req.rtype, req.value)
                dstRes = srcRes.withold(req)
                #  reqResMap[req] = (srcRes, dstRes)
                job._resourceRequest[req] = (srcRes, dstRes)
                srcRes.addUser(job)
        except:
            #  for srcRes, dstRes in reqResMap.values():
            for req, x in job._resourceRequest.items():
                if x is None:
                    continue
                (srcRes, dstRes) = x
                srcRes.release(dstRes)
                srcRes.delUser(job)
                job._resourceRequest[req] = None
            raise RuntimeError(f"Resources allocation for {job.name} failed")
        #  job.setResources(reqResMap)
        self.jobsRunning.add(job)

    def free(self, job):
        for srcRes, dstRes in job.unsetResources():
            assert srcRes in self._resources
            srcRes.release(dstRes)
            srcRes.delUser(job)
        self.jobsRunning.remove(job)

    def scheduleJob(self, job):
        if self._jobScheduler is None:
            raise Exception(f"Machine {self.name} has no job scheduler")
        self._jobScheduler.schedule(job)

    def scheduleVM(self, job):
        if self._vmScheduler is None:
            raise Exception(f"Machine {self.name} has no VM scheduler")
        self._vmScheduler.schedule(job)

    def allocateVM(self, vm):
        if vm.host != self and vm.host is not None:
            raise Exception("Wrong host for given virtual machine")
        if vm in self._hostedVMs:
            raise Exception("This vm is already allocated")
        usedRes = []
        srcResMap = {}
        for req in vm.resourceRequest:
            srcRes = self.getBestFitting(req.rtype, req.value, excluded=usedRes)
            if req.value == INF and req.shared is False:
                req.value = srcRes.avaliableValue
            dstRes = srcRes.withold(req)
            usedRes += [srcRes]
            srcRes.vmsUsing.add(vm)
            srcResMap[dstRes] = srcRes
        vm.host = self
        vm.setResources(srcResMap)
        self._hostedVMs.add(vm)

    def freeVM(self, vm):
        if vm not in self._hostedVMs:
            raise Exception("This vm is allocated on a different machine")
        resources = vm.unsetResources()
        for res, srcRes in resources.items():
            if srcRes not in self._resources:
                raise Exception("Resource not found")
            srcRes.release(res)
            srcRes.vmsUsing.remove(vm)
        self._hostedVMs.remove(vm)
        vm.host = None

    def __lt__(self, other):
        return self._index < other._index

    def __eq__(self, other):
        if other is None:
            return False
        return self._index == other._index

    def __hash__(self):
        return self._index



class VirtualMachine(Machine):
    """
    Machine, that could be allocated on other machines,
    ald use part of it's resources to run jobs.
    """
    def __init__(self, name, resourceRequest=None,
                 getJobScheduler=lambda _: None,
                 getVMScheduler=lambda _: None,
                 host=None):
        super().__init__(name, {}, getJobScheduler, getVMScheduler)
        self.host = host
        self.resourceRequest = resourceRequest
        self._resources = []
        self._srcResMap = {}

    @property
    def maxResources(self):
        if len(self._resources) > 0:
            for res in self._resources:
                yield (res.rtype, res.maxValue)
        else:
            for req in self.resourceRequest:
                # TODO: request value of `inf`
                yield (req.rtype, req.value)

    def setResources(self, srcResMap):
        for res in srcResMap:
            self._resources += [res]
        self._srcResMap = srcResMap

    def unsetResources(self):
        srcResMap = self._srcResMap
        self._srcResMap = {}
        self._resources.clear()
        return srcResMap

