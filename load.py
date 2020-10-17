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

def tasksets_orig(rows):
    for row in rows:
        row = [ast.literal_eval(field) for field in row]
        yield as_object({
            'tasks'       : [as_object({
                'id'          : id,
                'period'      : period,
                'utilization' : util,
                'wcet'        : wcet,
                'segments'    : False,
            }) for (id, period, util, wcet) in row[0]],
            'total_util'  : row[1],
            'perc_util'   : row[2],
            'schedulable' : row[3] if len(row) >= 4 else False
        })

def taskset_dag(rows):
    tasks = []
    for row in rows:
        if row[0] == 'T':
            tasks.append(as_object({
                'id'       : ast.literal_eval((row[1])),
                'period'   : ast.literal_eval((row[2])),
                'deadline' : ast.literal_eval((row[3])),
                'segments' : [],
            }))
        elif row[0] == 'V':
            tid  = ast.literal_eval((row[1]))
            assert tasks[-1].id == tid
            tasks[-1].segments.append(as_object({
                'id'   : ast.literal_eval((row[2])),
                'wcet' : ast.literal_eval((row[3])),
                'predecessors' : [ast.literal_eval(f) for f in row[4:]],
            }))
        else:
            assert False # unknown input format
    for t in tasks:
        t.wcet = sum((s.wcet for s in t.segments))
    return as_object({
        'schedulable' : False, # no relevant information available
        'tasks'       : tasks,
    })


def tasksets(fname):
    with open(fname, 'r') as f:
        rows = list(csv.reader(f, delimiter=','))
        if rows[0][0] == 'T':
            # this looks like a DAG task set
            yield taskset_dag(rows)
        else:
            # must be a non-DAG task set
            return tasksets_orig(rows)

def jobs(task, horizon):
    for rel in range(0, horizon, task.period):
        if task.segments:
            segments = [as_object({
                'release' : rel,
                'deadline': rel + task.period,
                'cost'    : segment.wcet,
                'task'    : task,
                'seg_id'  : segment.id,
            }) for segment in task.segments]
            def by_id(id):
                return next((s for s in segments if s.seg_id == id))
            for s, ts in zip(segments, task.segments):
                s.predecessors = [by_id(id) for id in ts.predecessors]
            for s in segments:
                yield s
        else:
            yield as_object({
                'release' : rel,
                'deadline': rel + task.period,
                'cost'    : task.wcet,
                'task'    : task,
                'predecessors' : [],
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
