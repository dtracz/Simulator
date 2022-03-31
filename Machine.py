

class Resource:
    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.maxValue = value
        self.jobsUsing = set()

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

    def allocate(self, job):
        reqResource = job.reqResources[self.name]
        self.withold(reqResource.value)
        job.allocResources[self.name] = reqResource
        self.jobsUsing.add(job)

    def free(self, job):
        allocResource = job.allocResources[self.name]
        self.release(allocResource.value)
        del job.allocResources[self.name]
        self.jobsUsing.remove(job)



class Machine:
    def __init__(self, name, resources):
        self._name = name
        self._resources = resources

    def allocate(self, job):
        for name, resource in self._resources.items():
            if name in job.reqResources.keys():
                resource.allocate(job)

    def free(self, job):
        for name, resource in self._resources.items():
            if name in job.allocResources.keys():
                resource.free(job)

