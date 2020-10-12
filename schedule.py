#!/usr/bin/env python3

import argparse
import re
import os
import sys
import ast

from collections import defaultdict

from backfill import paf_backfill

import load
from load import as_object

ASSIGN_PATTERN = re.compile(r'^assign\[([0-9]+),([0-9]+)\] (.+)$', re.MULTILINE)
START_TIME = re.compile(r'^startTime\[([0-9]+)\] (.+)$', re.MULTILINE)
FINISH_TIME = re.compile(r'^finishTime\[([0-9]+)\] (.+)$', re.MULTILINE)

def allocations(mapping, start_times, finish_times):
    return [as_object({
        'id'    : i,
        'core'  : mapping[i],
        'start' : start_times[i],
        'end'   : finish_times[i],
    }) for i in sorted(mapping.keys())]


def load_solution(fname):
    sol = open(fname, 'r').read()
    if not 'Solution for model' in sol:
        return None

    # infer assignments
    mapping = {}
    for m in ASSIGN_PATTERN.finditer(sol):
        job_id  = int(m.group(1))
        core_id = int(m.group(2))
        # deal with floating point noise in the '1'
        flag    = int(round(ast.literal_eval(m.group(3))))
        if flag:
            mapping[job_id] = core_id

    # infer start times
    start_times = {}
    for m in START_TIME.finditer(sol):
        job_id  = int(m.group(1))
        stime = round(ast.literal_eval(m.group(2)), 2)
        start_times[job_id] = stime

    # infer finish times
    finish_times = {}
    for m in FINISH_TIME.finditer(sol):
        job_id  = int(m.group(1))
        ftime = round(ast.literal_eval(m.group(2)), 2)
        finish_times[job_id] = ftime

    return allocations(mapping, start_times, finish_times)

def heuristic_solution(schedule):
    # convert assignments
    mapping = {}
    start_times = {}
    finish_times = {}
    for core in schedule:
        for job, start in schedule[core]:
            mapping[job.id] = core
            start_times[job.id] = start
            finish_times[job.id] = start + job.cost

    return allocations(mapping, start_times, finish_times)

def show(opts, allocations, file=sys.stdout):
    print('%5s,%6s,%10s,%10s,%10s,%10s,%10s,%6s,%11s' % (
        'Job', 'Core', 'Start', 'End',
        'Release', 'Deadline', 'Cost',
        'Task', 'Job of Task'
    ), file=file)
    for j in allocations:
        print('%5d,%6d,%10.2f,%10.2f,%10.2f,%10.2f,%10.2f,%6d,%11d' % (
            j.id, j.core, j.start, j.end,
            j.job.release, j.job.deadline, j.job.cost,
            j.job.task.id, j.job.job_of_task,
        ), file=file)

def validate(all_jobs, allocations):
    assert len(all_jobs) == len(allocations)
    for j, alloc in zip(all_jobs, allocations):
        assert j.id == alloc.id
        alloc.job = j

    allocated = set()
    for alloc in allocations:
        allocated.add(alloc.job)
        # sanity check allocations
        assert alloc.start >= alloc.job.release
        assert alloc.end <= alloc.job.deadline
        assert alloc.end - alloc.start == alloc.job.cost
        # check for overlaps
        for other_alloc in allocations:
            if other_alloc != alloc and other_alloc.core == alloc.core:
                # make sure there is no overlap
                assert alloc.start >= other_alloc.end or \
                       other_alloc.start >= alloc.end
    for j in all_jobs:
        assert j in allocated
    assert len(allocated) == len(all_jobs)

def process(opts, fname):
    bname = os.path.basename(fname)
    m = re.match('([0-9]+)Cores', bname)
    if m is None and opts.number_of_cores is None:
        print('%s: Could not infer number of cores (specify with -m)' % fname)
        return
    elif m is None:
        ncores = opts.number_of_cores
    else:
        ncores = int(m.group(1))

    if not opts.compare:
        print('Processing %s...' % fname)

    odir = opts.output_dir if opts.output_dir else os.path.dirname(fname)
    os.makedirs(odir, exist_ok=True)

    id = 1
    for jobset in load.jobsets(fname):
        name = bname.replace('.csv', '') + ('-ID%03d' % id)
        id += 1

        if not opts.job_set_index is None and id - 1 != opts.job_set_index:
            continue

        for i, j in enumerate(jobset.jobs):
            j.id = i

        sched_name = os.path.join(odir, name + '-schedule.csv')

        allocations = None
        # first, try inferring a schedule from a MILP solution
        if opts.load_milp_sol:
            sol_fname = os.path.join(opts.solutions_dir, name + '.sol')
            if os.path.exists(sol_fname):
                allocations = load_solution(sol_fname)
                if opts.compare:
                    if allocations and not os.path.exists(sched_name):
                        print(name, 'solved by MILP solver')
                    continue
                elif not allocations:
                    print('%s: infeasible.' % name)
                    continue

        if opts.compare:
            if not os.path.exists(sched_name) and jobset.taskset.schedulable:
                print(name, 'solved by prior heuristics')
            continue

        if not allocations and opts.heuristic:
            print('Trying to schedule %s (%d jobs)...' % (name, len(jobset.jobs)))
            (unassigned, schedule, _) = paf_backfill(jobset.jobs, ncores)
            if not unassigned:
                allocations = heuristic_solution(schedule)

        if allocations:
            validate(jobset.jobs, allocations)

            task_counter = defaultdict(int)
            for alloc in allocations:
                task_counter[alloc.job.task.id] += 1
                alloc.job.job_of_task = task_counter[alloc.job.task.id]

            with open(sched_name, 'w') as f:
                show(opts, allocations, file=f)
        else:
            print('%s: no solution found.' % name)

def parse_args():
    parser = argparse.ArgumentParser(
        description="MILP result interpretation tool")

    parser.add_argument('input_files', nargs='*',
        metavar='INPUT',
        help='input task sets (*.csv)')

    parser.add_argument('-o', '--output-dir', default='./Schedules',
                        action='store',
                        help='where to store the generated schedules')

    parser.add_argument('-m', '--number-of-cores', default=None,
                        action='store', type=int, metavar='M',
                        help='number of cores to assume (if not inferred from file name)')

    parser.add_argument('-i', '--job-set-index', default=None,
                        action='store', type=int, metavar='ID',
                        help='look only at a specific index in the task set file')

    parser.add_argument('-s', '--solutions-dir', default='./Results',
                        action='store', metavar='DIR',
                        help='where to find the MILP solutions, if any')

    parser.add_argument('--heuristic', default=None,
                        action='store_true',
                        help='run a scheduling heuristic')

    parser.add_argument('--compare', default=None,
                        action='store_true',
                        help='compare against schedulability flag')

    parser.add_argument('-l', '--load-milp-sol', default=None,
                        action='store_true',
                        help='infer schedule from a *.sol file')

    return parser.parse_args()

def main():
    opts = parse_args()

    for f in opts.input_files:
        process(opts, f)

if __name__ == '__main__':
    main()
