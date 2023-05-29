import numpy as np
import torch
from toolkit import *
from Simulator import *
from Events import *
from Resource import *
from Machine import *
from Job import *
from scheduling.BaseSchedulers import *


class VMPlacementPolicyRandom(VMPlacementPolicySimple):
    def __init__(self, machines):
        super().__init__(machines)
        self._machines = list(machines)[:]

    def placeVM(self, vm):
        np.random.shuffle(self._machines)
        for machine in self._machines:
            if machine.isFittable(vm):
                scheduler = self._schedulers[machine]
                scheduler.schedule(vm)
                return
        raise Exception(f"Non of the known machines is suitable for {vm.name}")



class VMPlacementPolicyAI(VMPlacementPolicySimple):
    def __init__(self, machines, ModelClass=None):
        super().__init__(machines)
        self._machines = list(machines)[:]
        self._taskInfoSize = 6
        self._machineInfoSize = 2*self._taskInfoSize + 3 + 3
        self._model = ModelClass((len(self._machines), self._machineInfoSize),
                                 self._taskInfoSize, len(self._machines))

    @staticmethod
    def _getTaskInfo(task):
        """
        Returns np.array of
        [cpu length, gpu length, number of threads,
        number of gpus, requested ram size, priority]
        of given task.
        """
        request = task._resourceRequest
        rams = list(filter(lambda r: r.rtype == RType.RAM, request))
        assert len(rams) == 1
        cores = list(filter(lambda r: r.rtype == RType.CPU_core, request))
        gpus = list(filter(lambda r: r.rtype == RType.GPU, request))
        jobs = task._jobScheduler._jobQueue
        assert len(jobs) > 0
        cpu_ops = sum([job.operations.get(RType.CPU_core, 0) for job in jobs])
        gpu_ops = sum([job.operations.get(RType.GPU, 0) for job in jobs])
        priority = sum([job.priority for job in jobs])
        info = [cpu_ops, gpu_ops, len(cores), len(gpus), rams[0].value, priority]
        return np.array(info)

    def _getMachineInfo(self, machine):
        """
        Returns np.array of
        [number of cores, number of gpus, ram size,
        number of awaiting tasks, their total cpu and gpu lengths,
        and means and standard deviations of awaiting tasks parameters]
        """
        rams = list(filter(lambda r: r.rtype == RType.RAM, machine.resources))
        assert len(rams) == 1
        cores = list(filter(lambda r: r.rtype == RType.CPU_core, machine.resources))
        assert len(cores) > 0
        gpus = list(filter(lambda r: r.rtype == RType.GPU, machine.resources))
        resource_info = [len(cores), len(gpus), rams[0].value]
        vms = machine._vmScheduler.vms
        if len(vms) > 0:
            vms_data = np.array([self._getTaskInfo(vm) for vm in vms])
            means = vms_data.mean(axis=0)
            stds = vms_data.std(axis=0)
            no_tasks = len(vms)
            cpu_length = vms_data[:,0].sum()
            gpu_length = vms_data[:,1].sum()
            add_info = [no_tasks, cpu_length, gpu_length]
            vms_info = np.concatenate((add_info, means, stds))
        else:
            vms_info = (2*self._taskInfoSize + 3)*[0]
        info = np.concatenate((resource_info, vms_info))
        return info

    def placeVM(self, vm):
        task_info = self._getTaskInfo(vm)
        state_info = [self._getMachineInfo(machine) for machine in self._machines]
        state_info = np.array(state_info)
        # add batch dimension
        state_info = np.expand_dims(state_info, 0)
        task_info = np.expand_dims(task_info, 0)
        scores = self._model.predict((state_info, task_info))
        # remove batch dimension
        scores = scores[0]
        ordered_indices = np.argsort(-scores)
        for i in ordered_indices:
            machine = self._machines[i]
            if machine.isFittable(vm):
                scheduler = self._schedulers[machine]
                scheduler.schedule(vm)
                return
        raise Exception(f"Non of the known machines is suitable for {vm.name}")

