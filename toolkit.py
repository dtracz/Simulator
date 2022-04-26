from sortedcontainers import SortedDict, SortedSet


class MultiDictRevDict:
    """
    Bidirectional map with multiple value per one key feature
    and ordered by key in one side, but unordered and single
    value per key in reverse.
    """

    class ItemsIterator:
        def __init__(self, outerIter):
            self._outerIter = outerIter
            self._currentKey = None
            self._innerIter = None

        def __next__(self):
            obj = None
            while obj is None:
                try:
                    obj = next(self._innerIter)
                except (TypeError, StopIteration):
                    key, val = next(self._outerIter)
                    self._currentKey = key
                    self._innerIter = iter(val)
            return (self._currentKey, obj)


    def __init__(self):
        self._fwdDict = SortedDict()
        self._revDict = {}
    
    def add(self, key, val):
        if val in self._revDict.keys():
            raise RuntimeError("value already present")
        if key not in self._fwdDict.keys():
            self._fwdDict[key] = SortedSet()
        self._fwdDict[key].add(val)
        self._revDict[val] = key

    def remove(self, val):
        key = self._revDict[val]
        del self._revDict[val]
        vals = self._fwdDict[key]
        vals.remove(val)
        if len(vals) == 0:
            del self._fwdDict[key]
    
    def __len__(self):
        return len(self._revDict)

    __setitem__ = add
    __delitem__ = remove

    def popitem(self):
        key, vals = self._fwdDict.peekitem(index=0)
        val = vals[0]
        self.remove(val)
        return (key, val)

    def atMax(self, key):
        sset = self._fwdDict[key]
        return max(sset, key=lambda x: x.value)

    def __iter__(self):
        outerIter = iter(self._fwdDict.items())
        return MultiDictRevDict.ItemsIterator(outerIter)

    def getkey(self, val):
        return self._revDict[val]

    def clear(self):
        self._fwdDict.clear()
        self._revDict.clear()


