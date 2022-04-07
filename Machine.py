from Resource import *


class Infrastructure(dict):
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
    def __init__(self, name, resources):
        self.name = name
        self._resources = resources
        self._hostedVMs = set()

    def allocate(self, job):
        for name, resource in self._resources.items():
            if name in job.requestedRes.keys():
                resource.allocate(job)

    def free(self, job):
        for name, resource in self._resources.items():
            if name in job.obtainedRes.keys():
                resource.free(job)



class VirtualMachine(Machine):
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

