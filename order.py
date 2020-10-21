
from heapq import heappush, heappop

class ConsiderationOrder(object):

    def __init__(self, score_function, jobs=None):
        self.score_function = score_function
        self.queue = []
        if jobs:
            for j in jobs:
                self.add(j)

    def add(self, job):
        item = [self.score_function(job), job]
        heappush(self.queue, item)
        job.in_queue = item

    def update(self, job):
        job.in_queue[1] = None
        self.add(job)

    def next(self):
        job = None
        while self.queue and not job:
            _, job = heappop(self.queue)
        if job:
            job.in_queue = False
        return job
