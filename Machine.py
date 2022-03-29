

class Resource:
    def __init__(self, value):
        self.value = value
        self.maxValue = value

    def withold(self, value):
        if value > self.value:
            raise Exception(f"Requested {value} out of {self.value} avaliable")
        self.value -= value

    def release(self, value=float('inf')):
        if value == float('inf'):
            self.value = self.maxValue
        elif self.value + value <= self.maxValue:
            self.value += value
        else:
            raise Exception("Resource overflow after release")



class Machine:
    def __init__(self, name, resources):
        self._name = name
        self._resources = resources

    def withold(self, name, resource):
        if name not in self._resources:
            raise Exception(f"Machine {self._name} does not have any {name}")
        self._resources[name].withold(resource.value)

    def release(self, name, resource):
        if name not in self._resources:
            raise Exception(f"Machine {self._name} should not have any {name}")
        self._resources[name].release(resource.value)

