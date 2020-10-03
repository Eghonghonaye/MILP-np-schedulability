#Purpose of this file is to read in a task set, generate jobs for one hyper period and put them in the input data format needed for the MILP solver

import header
import read_tasks_from_file as rtff

#input data 

# jobs = []
# releaseTimes = []
# deadlines = []
# executionTimes = []
# processors = []
# #overlap variable
# M = 0

def read_task_sets(path):
	all_task_sets = rtff.main(path)
	return all_task_sets

def set_processors(num_cores):
	processors = ['i' for item in range(num_cores)]
	return processors

def set_overlap_var(task_set):
	hyperperiod = header.computeHyperperiod(task_set)
	M = 3*hyperperiod
	return M

def create_jobs(task_set):
	hyperperiod = header.computeHyperperiod(task_set)
	for task in task_set:
		task.jobs_in_hyper_period = hyperperiod/task.period;
		for jobnum in range(0,int(task.jobs_in_hyper_period)):
			job = header.Job(ex_time=task.ex_time, arr_time = (jobnum*task.period), abs_deadline = ((jobnum*task.period) + task.rel_deadline), job_number=jobnum, task_number=task.number)
			task.task_joblist.append(job)

def set_job_vars(task_set):
	jobs = []
	releaseTimes = []
	deadlines = []
	executionTimes = []

	for task in task_set:
		for job in task.task_joblist:
			jobs.append('a')
			releaseTimes.append(job.arr_time)
			deadlines.append(job.abs_deadline)
			executionTimes.append(job.ex_time)

	return jobs, releaseTimes, deadlines, executionTimes

def main(num_cores, path):
	all_task_sets = read_task_sets(path)
	processors = set_processors(num_cores)
	for task_set in all_task_sets:
		M = set_overlap_var(task_set)
		create_jobs(task_set)
		jobs, releaseTimes, deadlines, executionTimes = set_job_vars(task_set)
		
		yield jobs, releaseTimes, deadlines, executionTimes, processors, M

if __name__ == '__main__':
	main()




