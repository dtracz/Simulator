import numpy as np
import json, string
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from Job import *
from Resource import *
from Machine import *
from scheduling.BaseSchedulers import *


class JobGenerator(metaclass=ABCMeta):

    @staticmethod
    def createJob(operations, noCores, ramSize, gpus, machine=None, priority=1):
        req = [ResourceRequest(RType.RAM, ramSize)]
        for _ in range(noCores):
            req += [ResourceRequest(RType.CPU_core, INF)]
        for nCC in gpus:
            req += [ResourceRequest(RType.GPU, nCC, shared=False)]
        return Job(operations, req, machine, priority)

    @abstractmethod
    def getJobs(self, n=1, machine=None):
        pass



class RandomJobGenerator(JobGenerator):
    def __init__(self,
                 operations=lambda s: abs(np.random.normal(100, 50, s)),
                 noCores=lambda s: 1+np.random.binomial(7, 0.08, s),
                 ramSize=lambda s: np.random.uniform(0, 16, s),
                 noGPUs=lambda s: np.random.binomial(4, 0.05/7, s),
                 priorities=lambda s: np.ones(s),
                 defaultNCC=INF):
        self._operations = operations
        self._noCores = noCores
        self._ramSize = ramSize
        self._noGPUs = noGPUs
        self._nCC = defaultNCC
        self._priorities = priorities

    def getJobs(self, n=1, machine=None):
        operations = list(self._operations(n))
        noCores = self._noCores(n)
        ramSize = self._ramSize(n)
        noGPUs = self._noGPUs(n)
        priorities = self._priorities(n)
        for i in range(n):
            if noGPUs[i] > 0:
                operations[i] = {
                    RType.CPU_core: operations[i] / 10,
                    RType.GPU: operations[i] * 30,
                }
            job = self.createJob(operations[i], noCores[i], ramSize[i],
                                 noGPUs[i]*[self._nCC], machine, priorities[i])
            yield job
            


class FromFileJobGenerator(JobGenerator):
    def __init__(self, fname,
                 priorities=lambda s: np.ones(s)):
        self.fname = fname
        self.cpuSpeeds = {'xeon e3-1270 v5': 3.6}
        self.file = open(self.fname)
        self._priorities = priorities

    @staticmethod
    def _randWord(length):
        letters = string.ascii_lowercase
        return ''.join([np.random.choice(list(letters)) for i in range(length)])

    @staticmethod
    def _parseTime(report):
        time = report['stats']['cpu']['usage']
        if time[-1] == 's':
            time = time[:-1]
        time = float(time.strip())
        return time

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
        time = self._parseTime(jobReport)
        assert time > 0
        ops = self.cpuSpeeds[cpuType] * time
        noCores = int(jobRequest['limits']['cpus'])
        ramSize = int(jobRequest['limits']['memory'][:-1]) / 1e9
        return ops, noCores, ramSize

    def getJobs(self, n=1, machine=None):
        p = self._priorities(n)
        while n > 0:
            n -= 1
            try:
                line = next(self.file)
                ops, noCores, ramSize = self.parseLine(line)
            except StopIteration:
                break
            except Exception:
                n += 1
                continue
            job = self.createJob(ops, noCores, ramSize,
                                 gpus=[], machine=machine, priority=p[n])
            yield job



class CreateVM:

    @staticmethod
    def minimal(jobs,
                coreLimit=INF,
                scheduler=lambda m: JobSchedulerSimple(m, autofree=True),
                ownCores=False,
               ):
        name = "vm_for"
        noCores = 0
        ramSize = 0
        gpus = []
        for job in jobs:
            name += f"_{job.name}"
            currentNoCores = 0
            currentRamSize = 0
            for req in job.resourceRequest:
                if req.rtype is RType.CPU_core:
                    currentNoCores += 1
                if req.rtype is RType.RAM:
                    currentRamSize += req.value
            noCores = max(noCores, currentNoCores)
            ramSize = max(ramSize, currentRamSize)
            currentGPUs = list(filter(lambda r: r.rtype == RType.GPU,
                                      job.resourceRequest))
            for i in range(min(len(gpus), len(currentGPUs))):
                gpus[i].value = max(gpus[i].value, currentGPUs[i].value)
            gpus += currentGPUs[len(gpus):]
        noCores = min(noCores, coreLimit)
        req = [ResourceRequest(RType.RAM, ramSize)]
        for _ in range(noCores):
            req += [ResourceRequest(RType.CPU_core, INF, shared=not ownCores)]
        for gpu_req in gpus:
            gpu_req.shared = False
        req += gpus
        vm = VirtualMachine(name, req, scheduler)
        return vm



class VMDelayScheduler:
    def __init__(self, target, delay_dist=lambda s: np.zeros(s)):
        assert hasattr(target, 'scheduleVM')
        self._delay_dist = delay_dist
        self._target = target

    def scheduleVM(self, vms, target=None, delays=None, times=None):
        if target is None:
            target = self._target
        f = lambda x: x if isinstance(vms, Iterable) else [x]
        vms = f(vms)
        n = len(vms)
        assert delays is None or times is None
        if times is None:
            if delays is None:
                times = self._delay_dist(n) + NOW()
            else:
                times = f(delays) + NOW()
        times = f(times)
        sim = Simulator.getInstance()
        for time, vm in zip(times, vms):
            # make priorities 'start' at `time` instead of in 0
            for job in vm._jobScheduler._jobQueue:
                job._old_pr = job._priority
                job._priority = lambda t, f=job._old_pr, time=time: f(t - time)
            event = VMShedule(target, vm)
            sim.addEvent(time, event)

