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
        self._resources = resources
        self._hostedVMs = set()
        self.jobsRunning = set()
        self._jobScheduler = getJobScheduler(self)
        self._vmScheduler = getVMScheduler(self)
        Machine._noCreated += 1

    @property
    def resources(self):
        return self._resources

    @property
    def maxResources(self):
        for res in self.resources:
            yield (res.rtype, res.maxValue)

    def getBestFitting(self, rtype, value, excluded=[]):
        allRes = list(filter(lambda r: r.rtype == rtype, self.resources))
        if value == INF:
            f = lambda r: r.maxValue/(1 + r.noDynamicUses + len(r.vmsUsing))
            return max(allRes, key=f)
        allRes.sort(key=lambda r: r.value)
        for res in allRes:
            if res.value >= value:
                return res
        raise RuntimeError(f"Cannot find fitting {rtype}")

    def allocate(self, resHolder):
        #  reqResMap = {}
        try:
            for req in filter(lambda r: not r.shared, resHolder.resourceRequest):
                srcRes = self.getBestFitting(req.rtype, req.value)
                dstRes = srcRes.withold(req)
                #  reqResMap[req] = (srcRes, dstRes)
                resHolder._resourceRequest[req] = (srcRes, dstRes)
                srcRes.addUser(resHolder)
            for req in filter(lambda r: r.shared, resHolder.resourceRequest):
                srcRes = self.getBestFitting(req.rtype, req.value)
                dstRes = srcRes.withold(req)
                #  reqResMap[req] = (srcRes, dstRes)
                resHolder._resourceRequest[req] = (srcRes, dstRes)
                srcRes.addUser(resHolder)
        except:
            #  for srcRes, dstRes in reqResMap.values():
            for req, x in resHolder._resourceRequest.items():
                if x is None:
                    continue
                (srcRes, dstRes) = x
                srcRes.release(dstRes)
                srcRes.delUser(resHolder)
                resHolder._resourceRequest[req] = None
            raise RuntimeError(f"Resources allocation for {resHolder.name} failed")
        #  job.setResources(reqResMap)
        if isinstance(resHolder, VirtualMachine):
            self._hostedVMs.add(resHolder)
        else:
            self.jobsRunning.add(resHolder)

    def free(self, resHolder):
        for srcRes, dstRes in resHolder.unsetResources():
            assert srcRes in self.resources
            srcRes.release(dstRes)
            srcRes.delUser(resHolder)
        if isinstance(resHolder, VirtualMachine):
            self._hostedVMs.add(resHolder)
        else:
            self.jobsRunning.remove(resHolder)

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
        self.allocate(vm)
        vm.host = self

    def freeVM(self, vm):
        if vm not in self._hostedVMs:
            raise Exception("This vm is allocated on a different machine")
        self.free(vm)
        vm.host = None

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
                 getVMScheduler=lambda _: None,
                 host=None):
        Machine.__init__(self, name, [], getJobScheduler, getVMScheduler)
        ResourcesHolder.__init__(self, resourceRequest)
        self.host = host
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

