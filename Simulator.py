from sortedcontainers import SortedDict, SortedSet
from toolkit import MultiDictRevDict
from abc import ABCMeta, abstractmethod


class NotificationListener(metaclass=ABCMeta):
    def __new__(cls, *args, **kwargs):
        obj = super(NotificationListener, cls).__new__(cls)
        Simulator.getInstance().registerListener(obj)
        return obj

    @abstractmethod
    def notify(self, event):
        pass


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
        self._listeners = []
        self._eventQueue = Simulator.EventQueue()
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
        while len(self._eventQueue) > 0:
            time, event = self._eventQueue.proceed()
            for listener in self._listeners:
                listener.notify(event)
    
    def addEvent(self, time, event):
        self._eventQueue.addEvent(time, event)

    def removeEvent(self, event):
        self._eventQueue.removeEvent(event)

    def registerListener(self, listener):
        self._listeners += [listener]

    def __del__(self):
        self._listeners = []
        self._eventQueue.clear()
        Simulator.__self = None



