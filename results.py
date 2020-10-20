#!/usr/bin/env python3

import argparse
import re
import os
from enum import IntEnum

from collections import defaultdict

class Outcome(IntEnum):
    TIMEOUT    = 0
    FEASIBLE   = 1
    INFEASIBLE = 2
    INCOMPLETE = 3
    UNSOLVED   = 4

def parse_outcome(fname):
    with open(fname, 'r') as f:
        log = f.read()
        if 'Time limit reached' in log:
            return Outcome.TIMEOUT
        elif 'Model is infeasible' in log:
            return Outcome.INFEASIBLE
        elif 'Optimal solution found' in log:
            return Outcome.FEASIBLE
        else:
            # could not figure out outcome
            return Outcome.INCOMPLETE

FNAME_PATTERN = re.compile(r'([0-9]+)Cores([0-9]+)Tasks([0-9]+)-ID([0-9]+).*(\.log|-schedule\.csv|-schedule.nosol)')

def parse_config(fname):
    m = FNAME_PATTERN.match(os.path.basename(fname))
    assert m
    cores = int(m.group(1))
    tasks = int(m.group(2))
    util  = int(m.group(3))
    id    = int(m.group(4))
    if 'log' in m.group(5):
        kind = 'log'
    elif 'schedule.csv' in m.group(5):
        kind  = 'schedule'
    elif 'schedule.nosol' in m.group(5):
        kind  = 'failure-marker'
    else:
        assert False # don't know what to make of this
    return (cores, tasks, util, id, kind)

def count_results(opts):
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: ([], [], [], [], []))))
    for f in opts.input_files:
        cores, tasks, util, id, kind = parse_config(f)
        if kind == 'log':
            outcome = parse_outcome(f)
        elif kind == 'schedule':
            # in case of a schedule, existence implies feasibility
            outcome = Outcome.FEASIBLE
        else:
            # the heuristic couldn't solve it
            outcome = Outcome.UNSOLVED
        results[cores][tasks][util][outcome].append(id)
    return results

def print_results(opts, results):
    print("%6s,%6s,%5s,%9s,%18s,%11s,%10s,%11s,%6s,%18s" % (
        'Cores',
        'Tasks',
        'Util',
        'Feasible',
        'Heuristic-failed',
        'Infeasible',
        'Timed-out',
        'Incomplete',
        'Total',
        'Schedulability Ratio',
    ))
    for cores in sorted(results.keys()):
        for tasks in sorted(results[cores].keys()):
            for util in sorted(results[cores][tasks].keys()):
                feas    = len(results[cores][tasks][util][Outcome.FEASIBLE])
                infeas  = len(results[cores][tasks][util][Outcome.INFEASIBLE])
                timeout = len(results[cores][tasks][util][Outcome.TIMEOUT])
                incomp  = len(results[cores][tasks][util][Outcome.INCOMPLETE])
                unsolved = len(results[cores][tasks][util][Outcome.UNSOLVED])
                total = feas + infeas + timeout + incomp + unsolved if not opts.total else opts.total
                print("%6d,%6d,%5d,%9d,%18d,%11d,%10d,%11d,%6d,%18.4f" % (
                    cores,
                    tasks,
                    util,
                    feas,
                    unsolved,
                    infeas,
                    timeout,
                    incomp,
                    total,
                    feas / total
                ))

def print_all(opts, results):
    print("%6s,%6s,%5s,%4s,%9s,%17s,%11s,%10s" % (
        'Cores',
        'Tasks',
        'Util',
        'ID',
        'Feasible',
        'Heuristic-failed',
        'Infeasible',
        'Timed-out',
    ))
    for cores in sorted(results.keys()):
        for tasks in sorted(results[cores].keys()):
            for util in sorted(results[cores][tasks].keys()):
                all_ids = []
                for kind in Outcome:
                    for id in results[cores][tasks][util][kind]:
                        all_ids.append(id)
                for id in sorted(all_ids):
                    print("%6d,%6d,%5d,%4d,%9d,%17d,%11d,%10d" % (
                        cores,
                        tasks,
                        util,
                        id,
                        id in results[cores][tasks][util][Outcome.FEASIBLE],
                        id in results[cores][tasks][util][Outcome.UNSOLVED],
                        id in results[cores][tasks][util][Outcome.INFEASIBLE],
                        id in results[cores][tasks][util][Outcome.TIMEOUT],
                    ))


def list(opts):
    for fname in opts.input_files:
        outcome = parse_outcome(fname)
        if opts.list_feasible and outcome == Outcome.FEASIBLE:
            print(fname)
        if opts.list_infeasible and outcome == Outcome.INFEASIBLE:
            print(fname)
        if opts.list_timeouts and outcome == Outcome.TIMEOUT:
            print(fname)

def parse_args():
    parser = argparse.ArgumentParser(
        description="ILP result collation tool")

    parser.add_argument('input_files', nargs='*',
        metavar='SOLUTION-FILE',
        help='solution files (*.sol)')

    parser.add_argument('-f', '--list-feasible', default=False,
                        action='store_true',
                        help='output list of feasible solutions')

    parser.add_argument('-i', '--list-infeasible', default=False,
                        action='store_true',
                        help='output list of infeasible solutions')

    parser.add_argument('-t', '--list-timeouts', default=False,
                        action='store_true',
                        help='output list of models that timed out')

    parser.add_argument('-a', '--list-all', default=False,
                        action='store_true',
                        help="don't summarize results")

    parser.add_argument('--total', default=None,
                        action='store', type=int,
                        help='total to assume for schedulability purposes')

    return parser.parse_args()

def main():
    opts = parse_args()
    if opts.list_feasible or opts.list_infeasible or opts.list_timeouts:
        list(opts)
    else:
        r = count_results(opts)
        if opts.list_all:
            print_all(opts, r)
        else:
            print_results(opts, r)

if __name__ == '__main__':
    main()

