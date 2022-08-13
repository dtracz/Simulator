import numpy as np
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
        self._noVMs = list(machines)[:]

    def placeVM(self, vm):
        np.random.shuffle(self._noVMs)
        for machine in self._noVMs:
            if machine.isFittable(vm):
                scheduler = self._schedulers[machine]
                scheduler.schedule(vm)
                return
        raise Exception(f"Non of the known machines is suitable for {vm.name}")
    


class VMPlacementPolicyAI(VMPlacementPolicySimple):
    def __init__(self, machines):
        super().__init__(machines)
        self._noVMs = list(machines)[:]
        self._model = None #TODO

    @staticmethod
    def _getTaskInfo(task):
        """
        Returns np.array of
        [length, number of threads, requested ram size]
        of given task (vm with 1 awaiting job)
        """
        request = task._resourceRequest
        rams = list(filter(lambda r: r.rtype == RType.RAM, request))
        assert len(rams) == 1
        cores = list(filter(lambda r: r.rtype == RType.CPU_core, request))
        assert len(cores) > 0
        jobs = task._jobScheduler._jobQueue
        assert len(jobs) == 1
        info = [jobs[0].operations, len(cores), rams[0].value]
        return np.array(info)

    @staticmethod
    def _getMachineInfo(machine):
        """
        Returns np.array of [ram size, number of cores,
        number of awaiting tasks, theit total length per machine core,
        means and standard deviations of lengths, number of threads
        and requested ram sizes of awaiting tasks] (total 10 values)
        """
        rams = list(filter(lambda r: r.rtype == RType.RAM, machine.resources))
        assert len(rams) == 1
        cores = list(filter(lambda r: r.rtype == RType.CPU_core, machine.resources))
        assert len(cores) > 0
        resource_info = [len(cores), rams[0].value]
        vms = machine._vmScheduler.vms
        vms_data = np.array([self._getTaskInfo(vm) for vm in vms])
        op_mean, core_mean, ram_mean = vms_data.mean(axis=0)
        op_std, core_std, ram_std = vms_data.std(axis=0)
        no_tasks = vms_data.shape[0]
        total_length = vms_data[:,0].sum()
        vms_info = [no_tasks, total_length/len(cores),
                    op_mean, op_std, core_mean, core_std, ram_mean, ram_std]
        info = resource_info + vms_info
        return np.array(info)

    def placeVM(self, vm):
        state_info = [self._getMachineInfo(machine) for machine in self.machines]
        state_info = np.array(state_info)
        task_info = self._getTaskInfo(vm)
        scores = self._model(state_info, task_info)
        ordered_indices = np.argsort(-scores)
        for i in ordered_indices:
            machine = self.machines[i]
            if machine.isFittable(vm):
                scheduler = self._schedulers[machine]
                scheduler.schedule(vm)
                return
        raise Exception(f"Non of the known machines is suitable for {vm.name}")

