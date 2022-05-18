from numpy import random
import json, string
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



class FromFileJobGenerator:
    def __init__(self, fname):
        self.fname = fname
        self.cpuSpeeds = {'xeon e3-1270 v5': 3.6}
        self.file = open(self.fname)
        next(self.file)

    @staticmethod
    def createJob(operations, noCores, ramSize, machine=None):
        req = [ResourceRequest(Resource.Type.RAM, ramSize)]
        for _ in range(noCores):
            req += [ResourceRequest(Resource.Type.CPU_core, float('inf'))]
        return Job(operations, req, machine)

    @staticmethod
    def _randWord(length):
        letters = string.ascii_lowercase
        return ''.join([random.choice(list(letters)) for i in range(length)])

    def parseLine(self, line):
        tmp = ''
        while tmp in line:
            tmp = self._randWord(8)
        line = line.replace('""', tmp)
        parts = line.split('"')
        for i, part in enumerate(parts):
            parts[i] = part.replace(tmp, '"')
        jobRequest = json.loads(parts[1])
        jobReport = json.loads(parts[3])
        cpuType = jobRequest['requires'][0].split(':')[1]
        timeLimit = float(jobRequest['limits']['time'][:-1])
        ops = self.cpuSpeeds[cpuType] * timeLimit
        noCores = int(jobRequest['limits']['cpus'])
        ramSize = int(jobRequest['limits']['memory'][:-1]) / 1e9
        return ops, noCores, ramSize

    def getJobs(self, n=1, machine=None):
        for i in range(n):
            line = next(self.file)
            ops, noCores, ramSize = self.parseLine(line)
            job = self.createJob(ops, noCores, ramSize, machine)
            yield job

