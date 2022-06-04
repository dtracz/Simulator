import nose
from unittest import TestCase
from Simulator import *
from Resource import *
from Machine import *
from Job import *
 

class EventInspector(NotificationListener):
    def __init__(self, expected=[]):
        self._expectations = []
        for kwargs in expected:
            self.addExpectation(**kwargs)

    def addExpectation(self, f=lambda n: True, **kwargs):
        def verification(notify):
            verify = f(notify)
            for name, value in kwargs.items():
                verify *= hasattr(notify, name) and \
                          getattr(notify, name) == value
            return verify
        self._expectations += [verification]

    def notify(self, notification):
        for i, f in enumerate(self._expectations):
            if f(notification):
                del self._expectations[i]
                return

    def verify(self):
        assert 0 == len(self._expectations)



class SimulatorTests(TestCase):

    def setUp(self):
        Event._noCreated = 0
        Job._noCreated = 0
        Machine._noCreated = 0
        sim = Simulator.getInstance()
        assert sim.time == 0
        assert len(sim._eventQueue._todo) == 0
        assert len(sim._eventQueue._done) == 0

    def tearDown(self):
        sim = Simulator.getInstance()
        sim.clear()

