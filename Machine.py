from Resource import *


class Infrastructure(dict):
    """
    Infrastructure represents all hardware avaliable to run jobs.
    """
    __self = None

    def __init__(self):
        if Infrastructure.__self != None:
            raise Exception("Creating another instance of Infrastructure is forbidden")
        self.machine = super()
        Infrastructure.__self = self

    @staticmethod
    def getInstance(*args, **kwargs):
        if Infrastructure.__self == None:
            Infrastructure(*args, **kwargs)
        return Simulator.__self;

    def addMachine(self, machine):
        self[machine.name] = machine

    @property
    def machines(self):
        return list(self.values())



class Machine:
    """
    Hardware machine, that holds resources and is able
    to run jobs or host virtual machines.
    """
    def __init__(self, name, resources):
        self.name = name
        self._resources = resources
        self._hostedVMs = set()

    def allocate(self, job):
        for name in job.requestedRes.keys():
            self._resources[name].allocate(job)

    def free(self, job):
        for name in list(job.obtainedRes.keys()):
            self._resources[name].free(job)

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
        self.resourceRequest = resourceRequest

    def setResources(self, resources):
        self._resources = resources

    def unsetResources(self):
        resources = self._resources
        self._resources = {}
        return resources

