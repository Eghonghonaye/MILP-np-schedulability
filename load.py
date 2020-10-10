#!/usr/bin/env python3

import csv
import ast

from itertools import chain

try:
    from math import lcm
except ImportError:
    try:
        from math import gcd
    except ImportError:
        from fractions import gcd

    def lcm(a, b):
         return a * b // gcd(a, b)

def hyperperiod(periods):
    h = periods[0]
    for p in periods[1:]:
        h = lcm(h, p)
    return h

class as_object(object):
    def __init__(self, fields):
        self.__dict__ = fields
    def __str__(self):
        return str(self.__dict__)
    def __repr__(self):
        return "as_object(%s)" % repr(self.__dict__)

def tasksets(fname):
    with open(fname, 'r') as f:
        for row in csv.reader(f, delimiter=','):
            row = [ast.literal_eval(field) for field in row]
            yield as_object({
                'tasks'       : [as_object({
                    'id'          : id,
                    'period'      : period,
                    'utilization' : util,
                    'wcet'        : wcet,
                }) for (id, period, util, wcet) in row[0]],
                'total_util'  : row[1],
                'perc_util'   : row[2],
                'schedulable' : row[3] if len(row) >= 4 else False
            })


def jobs(task, horizon):
    for rel in range(0, horizon, task.period):
        yield as_object({
            'release' : rel,
            'deadline': rel + task.period,
            'task'    : task,
        })

def jobsets(fname):
    for ts in tasksets(fname):
        periods = [t.period for t in ts.tasks]
        ts.hyperperiod = hyperperiod(periods)
        jobset = sorted(chain.from_iterable((jobs(t, ts.hyperperiod)
                                             for t in ts.tasks)),
                        key=lambda job: job.release)
        yield as_object({
            'taskset' : ts,
            'jobs'    : jobset,
        })
