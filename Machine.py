from Event import *


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
        resource = job.requestedRes[self.name]
        self.withold(resource.value)
        job.obtainedRes[self.name] = resource
        self.jobsUsing.add(job)

    def free(self, job):
        resource = job.obtainedRes[self.name]
        self.release(resource.value)
        del job.obtainedRes[self.name]
        self.jobsUsing.remove(job)



class SharedResource(Resource):
    def withold(self, value):
        raise Exception("SharedResource cannot be reserved")

    def release(self, value):
        raise Exception("SharedResource cannot be reserved")

    def recalculateJobs(self, exc=[]):
        now = Simulator.getInstance().time
        for job in self.jobsUsing:
            if job in exc:
                continue
            jobRecalculate = JobRecalculate(job)
            Simulator.getInstance().addEvent(now, jobRecalculate)

    def allocate(self, job):
        resource = job.requestedRes[self.name]
        if resource.value != float('inf'):
            return super().allocate(job)
        self.jobsUsing.add(job)
        self.value = self.maxValue / len(self.jobsUsing)
        job.obtainedRes[self.name] = self
        self.recalculateJobs([job])

    def free(self, job):
        resource = job.obtainedRes[self.name]
        self.jobsUsing.remove(job)
        if len(self.jobsUsing) > 0:
            self.value = self.maxValue / len(self.jobsUsing)
        else:
            self.value = self.maxValue
        del job.obtainedRes[self.name]
        self.recalculateJobs([job])




class Machine:
    def __init__(self, name, resources):
        self._name = name
        self._resources = resources

    def allocate(self, job):
        for name, resource in self._resources.items():
            if name in job.requestedRes.keys():
                resource.allocate(job)

    def free(self, job):
        for name, resource in self._resources.items():
            if name in job.obtainedRes.keys():
                resource.free(job)

