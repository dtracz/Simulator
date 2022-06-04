import nose
from unittest import TestCase
from Simulator import *
from Resource import *
from Machine import *
from Job import *
 

class EventInspector(NotificationListener):
    def __init__(self, expected=[]):
        self._expectations = []
        for time, name in expected:
            self.addExpected(time, name)

    def addExpected(self, time, name):
        self._expectations += [
            lambda e: e.name == name and \
                      Simulator.getInstance().time == time,
        ]

    def notify(self, notification):
        if not hasattr(notification, 'event'):
            return
        event = notification.event
        for i, f in enumerate(self._expectations):
            if f(event):
                del self._expectations[i]
                break

    def verify(self):
        assert 0 == len(self._expectations)



class SimulatorTests(TestCase):

    def setUp(self):
        Event._noCreated = 0
        Job._noCreated = 0
        Machine._noCreated = 0
        sim = Simulator.getInstance()
        assert sim.time == 0
        assert len(sim._eventQueue._todo) == 1
        assert len(sim._eventQueue._done) == 0

    def tearDown(self):
        sim = Simulator.getInstance()
        sim.clear()

