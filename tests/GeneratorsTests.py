import nose
from tests.base_test import *
from Simulator import *
from Resource import *
from Machine import *
from Job import *
from Generator import *


class GeneratorsTests(SimulatorTests):

    def test_simpleJobGeneration_det(self):
        gen = RandomJobGenerator(operations = lambda s: [10 for _ in range(s)],
                                 noCores = lambda s: [2 for _ in range(s)],
                                 ramSize = lambda s: [1 for _ in range(s)],
                                 noGPUs=lambda s: s*[0]
                                )
        jobs = gen.getJobs(10)
        for job in jobs:
            assert job.operations[RType.CPU_core] == 10
            coreReqs = list(filter(lambda r: r.rtype is RType.CPU_core,
                                   job.resourceRequest))
            assert len(coreReqs) == 2
            for coreReq in coreReqs:
                assert coreReq.value == INF
            ramReqs = list(filter(lambda r: r.rtype is RType.RAM,
                                  job.resourceRequest))
            assert len(ramReqs) == 1
            assert ramReqs[0].value == 1


    def test_simpleJobGeneration_rand(self):
        gen = RandomJobGenerator()
        jobs = gen.getJobs(1000)
        for job in jobs:
            assert all(n > 0 for n in job.operations.values())
            coreReqs = list(filter(lambda r: r.rtype is RType.CPU_core,
                                   job.resourceRequest))
            assert 1 <= len(coreReqs) and len(coreReqs) <= 8
            for coreReq in coreReqs:
                assert coreReq.value == INF
            ramReqs = list(filter(lambda r: r.rtype is RType.RAM,
                                  job.resourceRequest))
            assert len(ramReqs) == 1
            assert 0 < ramReqs[0].value and ramReqs[0].value <= 16


    def test_vmWrapping(self):
        gen = RandomJobGenerator()
        jobs = list(gen.getJobs(1000))
        vm = CreateVM.minimal(jobs)
        for job in jobs:
            assert vm.isFittable(job)


