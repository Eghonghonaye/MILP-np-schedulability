from itertools import permutations

class Task:
	def __init__(self, ex_time, rel_deadline, period, number, task_joblist, utilisation, phasing=0, jobs_in_hyper_period =0):
		self.ex_time = ex_time
		self.rel_deadline = rel_deadline
		self.period = period
		self.number = number
		self.phasing = phasing
		self.jobs_in_hyper_period = jobs_in_hyper_period
		self.core_number = 99
		''' for the scheduler'''
		self.next_job_number = 0
		self.priority = 99
		self.task_joblist = task_joblist
		self.utilisation = utilisation
		self.task_corelist = []
		'''for schedulability test'''
		self.blocking = 0
		self.WO = 0
		self.WCRT = 0

		#extensions for schedcat
		self.density = utilisation
		self.cost = ex_time
		self.deadline = rel_deadline

	def add_to_joblist(self,job):
		self.task_joblist.append(job)

	def remove_from_joblist(self,job):
		self.task_joblist.remove(job)


class Job:
	def __init__(self, ex_time, arr_time, abs_deadline, task_number, job_number, task_priority =99):
		self.ex_time = ex_time
		self.arr_time = arr_time
		self.abs_deadline = abs_deadline
		self.task_number = task_number
		self.task_priority = task_priority
		self.job_number = job_number
		self.offset = 0
		self.release_time = None
		self.starting_time = None
		self.finishing_time = None
		self.core_number = None

		#for offset tuning
		self.job_number_on_core = None

'''path item for the liquid path implemetaion'''
class PathItem:
	def __init__(self, job_number, task_number, ex_time, abs_deadline, est):
		self.est = est #earliest start time
		self.lst = 0 #latest start time
		self.start_time = 0
		self.end_time = 0
		self.task_number = task_number
		self.job_number = job_number
		self.abs_deadline = abs_deadline
		self.ex_time = ex_time

class Processor:
	def __init__(self, num_cores):
		self.num_cores = num_cores
		self.core_joblist = [[] for core in range(num_cores)]
		self.core_tasklist = [[] for core in range(num_cores)]
		self.core_utilisation = [0 for core in range(num_cores)]
		self.cores = [Core(item) for item in range(num_cores)]

	def assign_job_to_core(self, core_id, job):
		self.core_joblist[core_id].append(job)

	def assign_task_to_core(self, core_id, task):
		self.core_tasklist[core_id].append(task)
		self.core_utilisation[core_id] = self.core_utilisation[core_id] + task.utilisation
		

	def remove_job_from_core(self, core_id, job):
		self.core_joblist[core_id].remove(job)
		self.core_utilisation[core_id] = self.core_utilisation[core_id] - task.utilisation

	def remove_task_from_core(self, core_id, task):
		self.core_tasklist[core_id].remove(task)

class Core:
	def __init__(self, core_number):
		self.busy_time = 0
		self.number = core_number
		self.latest_start_time = 999999999
		self.status = "yes"
		self.task_list = []
		self.wake_up_time = 0
		self.path = [] #a list of objects path item for the liquid path thing

#lcm and gcd functions because they are not included in math for some weird reason
def gcd(a,b):
    """Compute the greatest common divisor of a and b"""
    while b > 0:
        a, b = b, a % b
    return a
    
def lcm(a, b):
    """Compute the lowest common multiple of a and b"""
    return a * b / gcd(a, b)


def processor_init(processor):
	processor.core_utilisation = [0 for core in range(processor.num_cores)]
	processor.core_joblist = [[] for core in range(processor.num_cores)]
	processor.core_tasklist = [[] for core in range(processor.num_cores)]
	processor.cores = [Core(item) for item in range(processor.num_cores)]


def processor_extend(processor):
	add_core_utilization = 0
	add_core_joblist = []
	add_core_tasklist = []

	processor.num_cores = processor.num_cores + 1
	processor.core_utilisation.append(add_core_utilization)
	processor.core_joblist.append(add_core_joblist)
	processor.core_tasklist.append(add_core_tasklist)


def computeHyperperiod(task_set):
	period_list = []
	for i in range(len(task_set)):
		period_list.append(task_set[i].period)
	if period_list:
		hyper_period = period_list[0]

		for item in period_list[1:]:
			hyper_period = lcm(hyper_period, item)

	return hyper_period


def createTaskJobs(task_set,hyper_period):
	#print "Creating Task Jobs"
	for item in task_set:
		#compute number of jobs in hyper period
		item.jobs_in_hyper_period = hyper_period/item.period		
		item.task_joblist = []
		for i in range(int(item.jobs_in_hyper_period)):
			job = Job(ex_time=item.ex_time, arr_time = (i*item.period), abs_deadline = ((i*item.period) + item.rel_deadline), job_number=i, task_number=item.number)
			item.task_joblist.append(job)
			#print "Task" + str(item.number) + " " + str(job.arr_time)

def jobArrivals(task_set,time_frame):
	arrived_jobs = []
	for task in task_set:
		for job in task.task_joblist:
			#check all jobs that should be available by then
			#print "Job" + str(job.job_number) + " of " + str(job.task_number)
			if job.arr_time in range(0,time_frame):
				arrived_jobs.append(job)
	return arrived_jobs

		

