#!/usr/bin/env python3

import argparse
import re
import os

import milpForm
import parser

def main():
	number_of_cores = int(sys.argv[1])
	number_of_tasks = int(sys.argv[2])
	for utilisation in [90,80,70,60,50,40,30,20,10]:
		path = "TaskSets/" + str(number_of_cores) + "Cores" + str(number_of_tasks) + "Tasks" + str(utilisation) + ".csv"
		resultPath = "MILPresults" + str(number_of_cores) + "Cores" + str(number_of_tasks) + "Tasks"

		for jobs, releaseTimes, deadlines, executionTimes, processors, M in parser.main(number_of_cores,path):
			status = milpForm.runExperiment(jobs, releaseTimes, deadlines, executionTimes, processors, M)
			lg.log_results(resultPath, [utilisation,status])

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
    for _jobs, releaseTimes, deadlines, executionTimes, _procs, M in parser.main(ncores, fname):
        print('Preparing model #%d...' % id)
        milp = milpForm.makeModel(releaseTimes, deadlines, executionTimes, ncores, M)
        model_fname = os.path.join(odir, bname.replace('.csv', '') + ('-ID%03d.%s' % (id, opts.format)))
        print('Writing %s...' % model_fname)
        milp.write(model_fname)
        id += 1

def parse_args():
    parser = argparse.ArgumentParser(
        description="ILP Generation Tool")

    parser.add_argument('input_files', nargs='*',
        metavar='INPUT',
        help='input files (*.csv)')

    parser.add_argument('-m', '--number-of-cores', default=None,
                        action='store', type=int,
                        help='number of cores to assume (if not inferred from file name)')

    parser.add_argument('-o', '--output-dir', default=None,
                        action='store',
                        help='where to store the generated problems')

    parser.add_argument('-f', '--format', default='lp',
                        choices=['lp', 'mps'],
                        help='what output format to generate')

    return parser.parse_args()

def main():
    opts = parse_args()

    for f in opts.input_files:
        process(opts, f)

if __name__ == '__main__':
    main()

