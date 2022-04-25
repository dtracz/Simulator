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
    _noMachines = 0

    def __init__(self, name, resources,
                 getJobScheduler=lambda _: None,
                 getVMScheduler=lambda _: None):
        self._index = Machine._noMachines
        self.name = name
        self._resources = self.makeResources(resources)
        self._hostedVMs = set()
        self.jobsRunning = set()
        self._jobScheduler = getJobScheduler(self)
        self._vmScheduler = getVMScheduler(self)
        Machine._noMachines += 1

    @staticmethod
    def makeResources(resIterable):
        resources = MultiDictRevDict()
        for resource in resIterable:
            resources.add(resource.rtype, resource)
        return resources

    def allocate(self, job):
        for name in job.resourceRequest.keys():
            self._resources.atLast(name).allocate(job)

    def free(self, job):
        for name in list(job.obtainedRes.keys()):
            self._resources.atLast(name).free(job)

    def scheduleJob(self, job):
        if self._jobScheduler is None:
            raise Exception(f"Machine {self.name} has no job scheduler")
        self._jobScheduler.schedule(job)

    #  def scheduleVM(self, job):
    #      if self._vmScheduler is None:
    #          raise Exception(f"Machine {self.name} has no VM scheduler")
    #      self._vmScheduler.schedule(job)
    #
    #  def allocateVM(self, vm):
    #      if vm.host != self and vm.host is not None:
    #          raise Exception("Wrong host for given virtual machine")
    #      if vm in self._hostedVMs:
    #          raise Exception("This vm is already allocated")
    #      resources = {}
    #      for name, value in vm.resourceRequest.items():
    #          if name not in self._resources:
    #              raise IndexError(f"Machine {self.name} does not have"
    #                                "requested resource ({name})")
    #          resources[name] = self._resources[name].withold(value)
    #      vm.host = self
    #      vm.setResources(resources)
    #      self._hostedVMs.add(vm)
    #
    #  def freeVM(self, vm):
    #      if vm not in self._hostedVMs:
    #          raise Exception("This vm is allocated on a different machine")
    #      resources = vm.unsetResources()
    #      for name, resource in resources.items():
    #          self._resources[name].release(resource.value)
    #      self._hostedVMs.remove(vm)
    #      vm.host = None

    def __lt__(self, other):
        return self._index < other._index

    def __eq__(self, other):
        if other is None:
            return False
        return self._index == other._index

    def __hash__(self):
        return self._index



#  class VirtualMachine(Machine):
#      """
#      Machine, that could be allocated on other machines,
#      ald use part of it's resources to run jobs.
#      """
#      def __init__(self, name, resourceRequest=None,
#                   getJobScheduler=lambda _: None,
#                   getVMScheduler=lambda _: None,
#                   host=None):
#          super().__init__(name, {}, getJobScheduler, getVMScheduler)
#          self.host = host
#          self.resourceRequest = resourceRequest #{name: value}
#          self._resources = None
#
#      def setResources(self, resources):
#          self._resources = resources
#
#      def unsetResources(self):
#          resources = self._resources
#          self._resources = {}
#          return resources

