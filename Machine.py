from Resource import *


class Infrastructure:
    """
    Infrastructure represents all hardware avaliable to run jobs.
    """
    __self = None

    def __init__(self, machines, VMAllocatorClass):
        if Infrastructure.__self != None:
            raise Exception("Creating another instance of Infrastructure is forbidden")
        self.machines = set(machines)
        self._knownVMs = set()
        self._vmAlocator = VMAllocatorClass(self.machines, self._knownVMs)
        Infrastructure.__self = self

    @staticmethod
    def getInstance(*args, **kwargs):
        if Infrastructure.__self == None:
            Infrastructure(*args, **kwargs)
        return Simulator.__self;

    def scheduleVM(self, vm):
        self._vmAllocator.schedule(vm)

    def scheduleJob(self, job, vm):
        if vm not in self._knownVMs:
            raise Exception(f"Requested job on unknown vm {vm.name}")
        vm.schedule(job)


class Machine:
    """
    Hardware machine, that holds resources and is able
    to run jobs or host virtual machines.
    """
    def __init__(self, name, resources, scheduler=None):
        self.name = name
        self._resources = resources
        self._hostedVMs = set()
        self._scheduler = scheduler

    def allocate(self, job):
        for name in job.requestedRes.keys():
            self._resources[name].allocate(job)

    def free(self, job):
        for name in list(job.obtainedRes.keys()):
            self._resources[name].free(job)

    def schedule(self, job):
        self._scheduler.schedule(job)

    def allocateVM(self, vm):
        if vm.host != self and vm.host != None:
            raise Exception("Wrong host for given virtual machine")
        if vm in self._hostedVMs:
            raise Exception("This vm is already allocated")
        resources = {}
        for name, value in vm.resourceRequest.items():
            if name not in self._resources:
                raise IndexError(f"Machine {self.name} does not have"
                                  "requested resource ({name})")
            resources[name] = self._resources[name].withold(value)
        vm.host = self
        vm.setResources(resources)
        self._hostedVMs.add(vm)

    def freeVM(self, vm):
        if vm not in self._hostedVMs:
            raise Exception("This vm is allocated on a different machine")
        resources = vm.unsetResources()
        for name, resource in resources.items():
            self._resources[name].release(resource.value)
        self._hostedVMs.remove(vm)
        vm.host = None



class VirtualMachine(Machine):
    """
    Machine, that could be allocated on other machines,
    ald use part of it's resources to run jobs.
    """
    def __init__(self, name, resourceRequest=None, host=None):
        super().__init__(name, {})
        self.host = host
        self.resourceRequest = resourceRequest #{name: value}
        self._resources = None

    def setResources(self, resources):
        self._resources = resources

    def unsetResources(self):
        resources = self._resources
        self._resources = {}
        return resources

