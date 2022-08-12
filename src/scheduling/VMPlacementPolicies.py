from toolkit import *
from Simulator import *
import numpy as np
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
    
