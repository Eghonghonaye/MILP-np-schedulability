
from heapq import heappush, heappop

class ConsiderationOrder(object):

    def __init__(self, score_function, jobs=None):
        self.score_function = score_function
        self.queue = []
        if jobs:
            for j in jobs:
                self.add(j)

    def add(self, job):
        item = [self.score_function(job), job.id, job]
        heappush(self.queue, item)
        job.in_queue = item

    def update(self, job):
        del job.in_queue[2]
        self.add(job)

    def next(self):
        job = None
        while self.queue and not job:
            item = heappop(self.queue)
            if len(item) > 2:
                job = item[2]
                job.in_queue = False

        return job
