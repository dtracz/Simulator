from sortedcontainers import SortedDict, SortedSet
import json


INF = float('inf')
EPS = 1e-10


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

    def getAll(self, key):
        return self._fwdDict[key]

    def __iter__(self):
        outerIter = iter(self._fwdDict.items())
        return MultiDictRevDict.ItemsIterator(outerIter)

    def hasValue(self, val):
        return val in self._revDict

    def getkey(self, val):
        return self._revDict[val]

    def clear(self):
        self._fwdDict.clear()
        self._revDict.clear()



class Map(SortedDict):

    def first_key_lower(self, key):
        idx = self._list.bisect_left(key)
        if idx == 0:
            raise KeyError(f"{self} does not contain key lower than {key}")
        key_ = self._list[idx-1]
        return key_

    def first_key_higher(self, key):
        idx = self._list.bisect_right(key)
        if idx == len(self._list):
            raise KeyError(f"{self} does not contain key higher than {key}")
        key_ = self._list[idx]
        return key_


def dictMinus(d0, d1):
    ans = {}
    for i in set(d0.keys()).union(d1.keys()):
        ans[i] = d0.get(i, 0) - d1.get(i, 0)
    return ans


def dictPlus(d0, d1):
    ans = {}
    for i in set(d0.keys()).union(d1.keys()):
        ans[i] = d0.get(i, 0) + d1.get(i, 0)
    return ans


def dictMultiply(a, d):
    ans = {}
    for i in d.keys():
        ans[i] = a * d[i]
    return ans



class Global:
    class Val:
        pass

    @classmethod
    def load(cls, filename):
        with open(filename, 'r') as file:
            file_data = json.load(file)
        cls._load(file_data, cls)

    @classmethod
    def _load(cls, inp, obj=None):
        if type(inp) is list:
            return cls._load_list(inp, obj)
        if type(inp) is dict:
            return cls._load_dict(inp, obj)
        return inp

    @classmethod
    def _load_list(cls, inp_list, obj=None):
        if obj is None:
            obj = []
        for elem in inp_list:
            obj += [cls._load(elem)]
        return obj

    @classmethod
    def _load_dict(cls, inp_dict, obj=None):
        if obj is None:
            obj = cls.Val()
        for key, inp_value in inp_dict.items():
            value = cls._load(inp_value)
            setattr(obj, key, value)
        return obj

