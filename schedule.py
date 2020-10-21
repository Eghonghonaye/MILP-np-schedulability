#!/usr/bin/env python3

import argparse
import re
import os
import sys
import ast

from collections import defaultdict

import backfill
import feasint
import dagfill
import dagfeasint

import cProfile

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
        j.alloc = alloc

    allocated = set()
    # check that all allocations are in the corresponding job's
    # feasibility window and non-overlapping
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
    # make sure no job was missed
    for j in all_jobs:
        assert j in allocated
    # make sure the schedule is DAG-compliant
    for j in all_jobs:
        # all predecessors must finish before this job's start
        for p in j.predecessors:
            assert p.alloc.end <= j.alloc.start
    assert len(allocated) == len(all_jobs)

def process(opts, fname):
    bname = os.path.basename(fname)
    try:
        ncores = int(next(re.finditer('([0-9]+)Cores', fname)).group(1))
    except StopIteration:
        if opts.number_of_cores is None:
            print('%s: Could not infer number of cores (specify with -m)' % fname)
            return
        else:
            ncores = opts.number_of_cores

    if not opts.compare:
        print('Processing %s...' % fname)

    odir = opts.output_dir if opts.output_dir else os.path.dirname(fname)
    os.makedirs(odir, exist_ok=True)

    id = 1
    for jobset in load.jobsets(fname):
        if bname.startswith('Run'):
            name = os.path.basename(fname.replace('/Run_', '-ID')).replace('.csv', '')
        else:
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

        def run_heuristic():
            print('Trying to schedule %s (%d jobs)...' % (name, len(jobset.jobs)))
            if jobset.is_dag and opts.heuristic == 'backfill':
                (unassigned, schedule, _) = dagfill.paf_meta_heuristic(jobset.jobs, ncores)
            elif jobset.is_dag and opts.heuristic == 'feasint':
                (unassigned, schedule, _) = dagfeasint.paf_meta_heuristic(jobset.jobs, ncores)
            elif not jobset.is_dag and  opts.heuristic == 'backfill':
                (unassigned, schedule, _) = backfill.paf_meta_heuristic(jobset.jobs, ncores)
            elif not jobset.is_dag and opts.heuristic == 'feasint':
                (unassigned, schedule, _) = feasint.paf_meta_heuristic(jobset.jobs, ncores)
            else:
                assert False

            if not unassigned:
                return heuristic_solution(schedule)
            else:
                return None

        if not allocations and opts.heuristic:
            if opts.profile:
                with cProfile.Profile() as pr:
                    allocations = run_heuristic()
                pr.print_stats('cumulative')
            else:
                allocations = run_heuristic()

        if allocations:
            validate(jobset.jobs, allocations)

            task_counter = defaultdict(int)
            for alloc in allocations:
                task_counter[alloc.job.task.id] += 1
                alloc.job.job_of_task = task_counter[alloc.job.task.id]

            with open(sched_name, 'w') as f:
                show(opts, allocations, file=f)
            print('%s: solution stored in %s' % (name, sched_name))
        else:
            print('%s: no solution found.' % name)
            if opts.log_failures:
                f = open(sched_name.replace('.csv', '.nosol'), 'w')
                f.write('no solution found')
                f.close()

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
                        action='store',
                        choices=['backfill', 'feasint'],
                        help='run a scheduling heuristic')

    parser.add_argument('-f', '--log-failures', default=None,
                        action='store_true',
                        help='write *.nosol failure indicators')

    parser.add_argument('--compare', default=None,
                        action='store_true',
                        help='compare against schedulability flag')

    parser.add_argument('--profile', default=None,
                        action='store_true',
                        help="run Python's cProfile profiler")

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
