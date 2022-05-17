from numpy import random
from Job import *
from Resource import *


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
            
