import heapq as hq

class Simulator:
    class EventQueue:
        def __init__(self):
            self._currentTime = 0
            self._todo = []
            self._done = []

        def __len__(self):
            return len(self._todo)

        def proceed(self):
            time, event = hq.heappop(self._todo)
            self._currentTime = time
            event.proceed()
            self._done += (time, job)

        def addEvent(self, time, event):
            hq.heappush(self._todo, (time, event))


    __self = None

    def __init__(self):
        if Simulator.__self != None:
            raise Exception("Creating another instance of Simulator is forbidden")
        self._eventQueue = EventQueue()
        Simulator.__self = self
        
    @staticmethod
    def getInstance(*args, **kwargs):
        if Simulator.__self == None:
            Simulator(args, kwargs)
        return Simulator.__self;
    
    @property
    def time(self):
        return self._eventQueue.time

    def simulate(self):
        while len(self._eventQueue) > 0:
            self._eventQueue.proceed()
    
    def addEvent(self, time, event):
        self._eventQueue.addEvent(time, event)

