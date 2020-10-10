#!/usr/bin/env python3

import argparse
import re
import os

import model
import load

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

    print('Processing %s...' % fname)

    odir = opts.output_dir if opts.output_dir else os.path.dirname(fname)
    os.makedirs(odir, exist_ok=True)

    id = 1
    for jobset in load.jobsets(fname):
        if opts.limit_job_sets and id > opts.limit_job_sets:
            print('Reached job set limit (%d), stopping.' % opts.limit_job_sets)
            break

        name = bname.replace('.csv', '') + ('-ID%03d' % id)
        id += 1

        if opts.propagate_results:
            if jobset.taskset.schedulable:
                # fake a successful result file
                flog = open(os.path.join(odir, '%s.%s' % (name, 'log')), 'w')
                print('Optimal solution found by heuristic', file=flog, flush=True)
                flog.close()
            continue

        if opts.skip_schedulable and jobset.taskset.schedulable:
            print('Skipping %s: a heuristic already deemed it feasible.' % name)
            continue

        print('Preparing model %s  (%d jobs)...' % (name, len(jobset.jobs)))
        releases  = [j.release for j in jobset.jobs]
        job_costs = [j.task.wcet for j in jobset.jobs]
        deadlines = [j.deadline for j in jobset.jobs]
        M = jobset.taskset.hyperperiod * 10 # "big M" constant
        milp = model.make_gurobi_milp(releases, deadlines, job_costs, ncores, M, name)

        model_fname = os.path.join(odir, '%s.%s' % (name, opts.format))
        print('Writing %s...' % model_fname)
        milp.write(model_fname)

def parse_args():
    parser = argparse.ArgumentParser(
        description="ILP Generation Tool")

    parser.add_argument('input_files', nargs='*',
        metavar='INPUT',
        help='input files (*.csv)')

    parser.add_argument('-l', '--limit-job-sets', default=None,
                        action='store', type=int,
                        help='maximum number of models to generate per configuration')

    parser.add_argument('-m', '--number-of-cores', default=None,
                        action='store', type=int,
                        help='number of cores to assume (if not inferred from file name)')

    parser.add_argument('-o', '--output-dir', default=None,
                        action='store',
                        help='where to store the generated problems')

    parser.add_argument('-f', '--format', default='lp',
                        choices=['lp', 'mps'],
                        help='what output format to generate')

    parser.add_argument('-s', '--skip-schedulable', default=False,
                        action='store_true',
                        help="don't generate MILPs for workloads found schedulable by a heuristic")

    parser.add_argument('-p', '--propagate-results', default=False,
                        action='store_true',
                        help="create dummy .log files for workloads found schedulable by a heuristic")

    return parser.parse_args()

def main():
    opts = parse_args()

    for f in opts.input_files:
        process(opts, f)

if __name__ == '__main__':
    main()

