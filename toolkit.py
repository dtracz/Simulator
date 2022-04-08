from sortedcontainers import SortedDict, SortedSet


class MultiDictRevDict:
    """
    Bidirectional map with multiple value per one key feature
    and ordered by key in one side, but unordered and single
    value per key in reverse.
    """
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

    def getkey(self, val):
        return self._revDict[val]

    def clear(self):
        self._fwdDict.clear()
        self._revDict.clear()


