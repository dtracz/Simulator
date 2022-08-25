from enum import Enum
from sortedcontainers import SortedDict, SortedSet
from toolkit import MultiDictRevDict
from abc import ABCMeta, abstractmethod


class Event:
    """
    Simple 0-argument function wrapper.
    Basic Event executed by Simulator.
    """
    _noCreated = 0

    def __init__(self, f, name=None, priority=0):
        self._f = f
        self._index = Event._noCreated
        if (name is None):
            name = f"Event_{self._index}"
        self.name = name
        self._priority = priority
        Event._noCreated += 1

    def proceed(self):
        self._f()

    def __lt__(self, other):
        if self._priority != other._priority:
            return self._priority > other._priority
        return self._index < other._index

    def __eq__(self, other):
        return self._index == other._index

    def __hash__(self):
        return self._index



class Notification:
    class Type(Enum):
        VMStart = 1
        VMEnd = 2
        JobStart = 3
        JobRecalculate = 4
        JobFinish = 5
        Other = 0

    def __init__(self, what, time='now', **kwargs):
        self.time = NOW() if time == 'now' else time
        self.what = what
        for name, value in kwargs.items():
            setattr(self, name, value)

NType = Notification.Type



class NotificationListener(metaclass=ABCMeta):
    def __new__(cls, *args, **kwargs):
        obj = super(NotificationListener, cls).__new__(cls)
        Simulator.getInstance().registerListener(obj)
        return obj

    @abstractmethod
    def notify(self, event):
        pass

    def unregister(self):
        Simulator.getInstance().unregisterListener(self)


class Simulator:
    """
    Singleton, the main Simulator class.
    Responsible for running simulation.
    Supports adding and removing future events."
    """
    class EventQueue:
        """
        Queue of future events stored in MultiDictRevDict structure.
        Stores past events in list [(time_executed, event)].
        Supports adding and removing future events,
        and porceeding the most recent one.

        """
        def __init__(self):
            self._currentTime = 0
            self._todo = MultiDictRevDict()
            self._done = []

        def __len__(self):
            return len(self._todo)

        def proceed(self):
            time, event = self._todo.popitem()
            self._currentTime = time
            event.proceed()
            self._done += [(time, event)]
            return self._done[-1]

        def addEvent(self, time, event):
            self._todo[time] = event

        def removeEvent(self, event):
            self._todo.remove(event)

        def clear(self):
            self._currentTime = 0
            self._todo.clear()
            self._done = []


    __self = None

    def __init__(self):
        if Simulator.__self != None:
            raise Exception("Creating another instance of Simulator is forbidden")
        self._listeners = set()
        self._to_unregister = set()
        self._eventQueue = Simulator.EventQueue()
        #  self.addEvent(self.time, Event(lambda: None, "SimulationStart", 1000))
        Simulator.__self = self
        
    @staticmethod
    def getInstance(*args, **kwargs):
        if Simulator.__self == None:
            Simulator(*args, **kwargs)
        return Simulator.__self;
    
    @property
    def time(self):
        return self._eventQueue._currentTime

    def simulate(self):
        self.emit(Notification(NType.Other, message="SimulationStart"))
        while len(self._eventQueue) > 0:
            time, event = self._eventQueue.proceed()

    def emit(self, notification):
        self.updateListeners()
        for listener in self._listeners:
            listener.notify(notification)
    
    def addEvent(self, time, event):
        self._eventQueue.addEvent(time, event)

    def removeEvent(self, event):
        self._eventQueue.removeEvent(event)

    def registerListener(self, listener):
        self._listeners.add(listener)

    def unregisterListener(self, listener):
        self._to_unregister.add(listener)

    def updateListeners(self):
        self._listeners = self._listeners.difference(self._to_unregister)
        self._to_unregister = set()

    def clear(self):
        self._listeners = []
        self._eventQueue.clear()
        Simulator.__self = None


def NOW():
    return Simulator.getInstance().time



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

    def allRegistered(self):
        return 0 == len(self._expectations)

    def verify(self):
        assert 0 == len(self._expectations)

