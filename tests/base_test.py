import nose
from unittest import TestCase
from Simulator import *
from Resource import *
from Machine import *
from Job import *


class SimulatorTests(TestCase):

    def setUp(self):
        Event._noCreated = 0
        Job._noCreated = 0
        Machine._noCreated = 0
        sim = Simulator.getInstance()
        assert sim.time == 0
        assert len(sim._eventQueue._todo) == 0
        assert len(sim._eventQueue._done) == 0
        assert len(sim._listeners) == 0

    def tearDown(self):
        sim = Simulator.getInstance()
        sim.clear()

