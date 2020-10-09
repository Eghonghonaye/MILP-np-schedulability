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
            assert False # could not figure out outcome

FNAME_PATTERN = re.compile(r'([0-9]+)Cores([0-9]+)Tasks([0-9]+)-ID([0-9]+).log')

def parse_config(fname):
    m = FNAME_PATTERN.match(os.path.basename(fname))
    assert m
    cores = int(m.group(1))
    tasks = int(m.group(2))
    util  = int(m.group(3))
    id    = int(m.group(4))
    return (cores, tasks, util, id)

def count_results(opts):
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: ([], [], []))))
    for f in opts.input_files:
        outcome = parse_outcome(f)
        cores, tasks, util, id = parse_config(f)
        results[cores][tasks][util][outcome].append(id)
    return results

def print_results(opts, results):
    print("%6s,%6s,%5s,%9s,%11s,%10s,%18s" % (
        'Cores',
        'Tasks',
        'Util',
        'Feasible',
        'Infeasible',
        'Timed-out',
        'Feasibility Ratio',
    ))
    for cores in sorted(results.keys()):
        for tasks in sorted(results[cores].keys()):
            for util in sorted(results[cores][tasks].keys()):
                feas    = len(results[cores][tasks][util][Outcome.FEASIBLE])
                infeas  = len(results[cores][tasks][util][Outcome.INFEASIBLE])
                timeout = len(results[cores][tasks][util][Outcome.TIMEOUT])
                print("%6d,%6d,%5d,%9d,%11d,%10d,%18.4f" % (
                    cores,
                    tasks,
                    util,
                    feas,
                    infeas,
                    timeout,
                    feas / (feas + infeas + timeout)
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

    return parser.parse_args()

def main():
    opts = parse_args()
    if opts.list_feasible or opts.list_infeasible or opts.list_timeouts:
        list(opts)
    else:
        r = count_results(opts)
        print_results(opts, r)

if __name__ == '__main__':
    main()

