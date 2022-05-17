from numpy import random
from Job import *
from Resource import *
from Machine import *
from Schedulers import *


class RandomJobGenerator:
    def __init__(self,
                 operations=lambda s: abs(random.normal(100, 50, s)),
                 noCores=lambda s: 1+random.binomial(7, 0.08, s),
                 ramSize=lambda s: random.uniform(0, 16, s)):
        self._operations = operations
        self._noCores = noCores
        self._ramSize = ramSize

    def getJobs(self, n=1, machine=None):
        operations = self._operations(n)
        noCores = self._noCores(n)
        ramSize = self._ramSize(n)
        for i in range(n):
            req = [ResourceRequest(Resource.Type.RAM, ramSize[i])]
            for _ in range(noCores[i]):
                req += [ResourceRequest(Resource.Type.CPU_core, float('inf'))]
            yield Job(operations[i], req, machine)
            


class CreateVM:

    @staticmethod
    def minimal(jobs,
                coreLimit=float('inf'),
                scheduler=lambda m: JobSchedulerSimple(m, autofree=True),
               ):
        name = "vm_for"
        noCores = 0
        ramSize = 0
        for job in jobs:
            name += f"_{job.name}"
            currentNoCores = 0
            currentRamSize = 0
            for req in job.resourceRequest:
                if req.rtype is Resource.Type.CPU_core:
                    currentNoCores += 1
                if req.rtype is Resource.Type.RAM:
                    currentRamSize += req.value
            noCores = max(noCores, currentNoCores)
            ramSize = max(ramSize, currentRamSize)
        noCores = min(noCores, coreLimit)
        req = [ResourceRequest(Resource.Type.RAM, ramSize)]
        for _ in range(noCores):
            req += [ResourceRequest(Resource.Type.CPU_core, float('inf'))]
        vm = VirtualMachine(name, req, scheduler)
        return vm

