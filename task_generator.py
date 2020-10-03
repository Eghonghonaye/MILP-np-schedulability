import numpy as np, numpy.random
from numpy.random import choice
import random
import header, csv, math
import result_logging as lg
from itertools import combinations

'''randFixSum

Library to generate random values that all add up to a value
Use to generate the utilisation of tasks


'''
def StaffordRandFixedSum(n, u, nsets):
    """
    Copyright 2010 Paul Emberson, Roger Stafford, Robert Davis.
    All rights reserved.
    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:
    1. Redistributions of source code must retain the above copyright notice,
        this list of conditions and the following disclaimer.
    2. Redistributions in binary form must reproduce the above copyright notice,
        this list of conditions and the following disclaimer in the documentation
        and/or other materials provided with the distribution.
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS ``AS IS'' AND ANY EXPRESS
    OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
    OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
    EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
    OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
    OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
    ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
    The views and conclusions contained in the software and documentation are
    those of the authors and should not be interpreted as representing official
    policies, either expressed or implied, of Paul Emberson, Roger Stafford or
    Robert Davis.
    Includes Python implementation of Roger Stafford's randfixedsum implementation
    http://www.mathworks.com/matlabcentral/fileexchange/9700
    Adapted specifically for the purpose of taskset generation with fixed
    total utilisation value
    Please contact paule@rapitasystems.com or robdavis@cs.york.ac.uk if you have
    any questions regarding this software.
    """
    if n < u:
        return None

    #deal with n=1 case
    if n == 1:
        return np.tile(np.array([u]), [nsets, 1])

    k = min(int(u), n - 1)
    s = u
    s1 = s - np.arange(k, k - n, -1.)
    s2 = np.arange(k + n, k, -1.) - s

    tiny = np.finfo(float).tiny
    huge = np.finfo(float).max

    w = np.zeros((n, n + 1))
    w[0, 1] = huge
    t = np.zeros((n - 1, n))

    for i in np.arange(2, n + 1):
        tmp1 = w[i - 2, np.arange(1, i + 1)] * s1[np.arange(0, i)] / float(i)
        tmp2 = w[i - 2, np.arange(0, i)] * s2[np.arange(n - i, n)] / float(i)
        w[i - 1, np.arange(1, i + 1)] = tmp1 + tmp2
        tmp3 = w[i - 1, np.arange(1, i + 1)] + tiny
        tmp4 = s2[np.arange(n - i, n)] > s1[np.arange(0, i)]
        t[i - 2, np.arange(0, i)] = (tmp2 / tmp3) * tmp4 + \
            (1 - tmp1 / tmp3) * (np.logical_not(tmp4))

    x = np.zeros((n, nsets))
    rt = np.random.uniform(size=(n - 1, nsets))  # rand simplex type
    rs = np.random.uniform(size=(n - 1, nsets))  # rand position in simplex
    s = np.repeat(s, nsets)
    j = np.repeat(k + 1, nsets)
    sm = np.repeat(0, nsets)
    pr = np.repeat(1, nsets)

    for i in np.arange(n - 1, 0, -1):  # iterate through dimensions
        # decide which direction to move in this dimension (1 or 0):
        e = rt[(n - i) - 1, ...] <= t[i - 1, j - 1]
        sx = rs[(n - i) - 1, ...] ** (1.0 / i)  # next simplex coord
        sm = sm + (1.0 - sx) * pr * s / (i + 1)
        pr = sx * pr
        x[(n - i) - 1, ...] = sm + pr * e
        s = s - e
        j = j - e  # change transition table column if required

    x[n - 1, ...] = sm + pr * s

    #iterated in fixed dimension order but needs to be randomised
    #permute x row order within each column
    for i in range(0, nsets):
        x[..., i] = x[np.random.permutation(n), i]

    return x.T.tolist()


def gen_randfixedsum(nsets, u, n):
    """
    Stafford's RandFixedSum algorithm implementated in Python.
    Based on the Python implementation given by Paul Emberson, Roger Stafford,
    and Robert Davis. Available under the Simplified BSD License.
    Args:
        - `n`: The number of tasks in a task set.
        - `u`: Total utilization of the task set.
        - `nsets`: Number of sets to generate.
    """
    return StaffordRandFixedSum(n, u, nsets)


def generateTask(task_number,task_period, task_utilisation):
	_period = task_period
	_ex_time = task_period * task_utilisation
	_rel_deadline = _period #assuming implicit deadlines
	_number = task_number
	_task_joblist=[]

	Task = header.Task(ex_time = _ex_time, 
		rel_deadline = _rel_deadline, 
		number = _number,
		period = _period,
		utilisation = task_utilisation,
		task_joblist = _task_joblist)

	return Task


''' a generator function to generate thetask list....idiomatic no?
Anywho, should be called idiomatically in a pyton list to get task_list

as in task_list = list(generateTaskSet(number_of_tasks))'''
def generateTaskSetLogUniform(number_of_tasks,period_list, period_weights, utilisation_set):
	for item in range(int(number_of_tasks)):
		'''task_period = (choice(period_list, p = period_weights))'''
		task_period = generateLogUniformPeriod(50, 10000, 50)
		task_utilisation = utilisation_set[0][item]
		Task = generateTask(item, task_period,task_utilisation)
		yield Task

def generateTaskSetAutomotive(number_of_tasks,period_list, period_weights, utilisation_set):
    for item in range(int(number_of_tasks)):
        task_period = (choice(period_list, p = period_weights))
        task_utilisation = utilisation_set[0][item]
        Task = generateTask(item, task_period,task_utilisation)
        yield Task

def generateTaskSetRandom(number_of_tasks,period_list, period_weights, utilisation_set):
    for item in range(int(number_of_tasks)):
        x = [1,2,3,4,5,6,7,8,9]
        y = [2,3,4]
        task_period = choice(x)*pow(10,choice(y))
        task_utilisation = utilisation_set[0][item]
        Task = generateTask(item, task_period,task_utilisation)
        yield Task


def generateLogUniformPeriod(minRange, maxRange, basePeriod):
	s = math.log(minRange)
	e = math.log(maxRange + basePeriod)
	'''From Mitra's code: here just generate a random value with normal distribution in range [s, e]'''
	'''random.gauss(mu, sigma) but this takes mu and sigma not a range
	and do we want a normal distribution or a uniform distribution?'''
	ri = np.random.uniform(low=s, high=e)
	'''is MU.floor same as math.floor???? '''
	period = (math.floor(math.exp(ri)/float(basePeriod)) * basePeriod)
	return period

''' unit test'''
def main(number_of_tasks, total_utilisation, distribution):
    '''define task set boundaries and properties'''

    '''period list is gotten from automated motors benchmark 
    weighted choice as in benchmark Emberson et. al'''
    am_periods = [10,20,50,100,200,500,1000,2000,10000]
    am_periods_weights = [0.03,0.02,0.02,0.25,0.25,0.03,0.20,0.01,0.04]
    '''add the angle synchronous tasks spread evenly among existing ones'''
    am_periods_weights = [item + (15/float(900)) for item in am_periods_weights]


    '''utilisation is gotten from staffords rand fixed sum below'''
    utilisation_set = gen_randfixedsum(1, total_utilisation, number_of_tasks)

    '''create list of tasks'''
    if distribution == "loguniform":
        task_list = list(generateTaskSetLogUniform(number_of_tasks,am_periods,am_periods_weights,utilisation_set))
        return task_list
    elif distribution == "automotive":
        task_list = list(generateTaskSetAutomotive(number_of_tasks,am_periods,am_periods_weights,utilisation_set))
        return task_list
    elif distribution == "random":
        task_list = list(generateTaskSetRandom(number_of_tasks,am_periods,am_periods_weights,utilisation_set))
        return task_list

def integerExTimes(task_set):
    for task in task_set:
        task.ex_time = int(task.ex_time)
        if (task.ex_time == 0):
            task.ex_time = 1
        task.utilisation = task.ex_time/float(task.period)

def uniNecTest(task_set):
    if sum([task.utilisation for task in task_set]) > 1:
        return False
    for task in task_set:
        for item in task_set:
            if task.number != item.number:
                #add an edge if the necessary condition is not satisfied
                if task.ex_time > 2*(item.period-item.ex_time):
                    return False
    return True

if __name__ == '__main__':
    '''number of tasks must not be > utilisation sum
    This algorithm protects against having tasks with utilisation > 1'''
    #uniprocessor generator
    '''for num_tasks in [5,8,10,15,20,32,40]:
        for i in range(60):
            for utilisation in range(1,10):
                util = float(utilisation)/10
                task_set = main(num_tasks,util,"automotive")
                integerExTimes(task_set)
                real_util = sum([task.utilisation for task in task_set])
                #check necessary test
                necessary = uniNecTest(task_set)
                while necessary==False:
                    task_set = main(num_tasks,util,"automotive")
                    integerExTimes(task_set)
                    real_util = sum([task.utilisation for task in task_set])
                    necessary = uniNecTest(task_set)

                task_list = [(task.number, task.period, task.utilisation, task.ex_time) for task in task_set]
                results = [task_list,real_util,num_tasks]
                lg.log_results("uniprocessor_test", results)'''
    number_of_cores = 16
    number_of_tasks = 32
    for i in range(300):
        for total_utilisation_perc in range(1,11):
            total_utilisation = (total_utilisation_perc/float(10)) * number_of_cores
            print (number_of_tasks, "tasks with utilisation", total_utilisation)

            task_set = main(number_of_tasks,total_utilisation,"automotive")
            test_hyper_period = header.computeHyperperiod(task_set)
            job_count = sum([test_hyper_period/task.period for task in task_set])

            #to discard tasks sets with > 10000 jobs in hyper period
            while job_count > 10000:
                task_set = main(number_of_tasks,total_utilisation,"automotive")
                test_hyper_period = header.computeHyperperiod(task_set)
                job_count = sum([test_hyper_period/task.period for task in task_set])

            task_list = [(task.number, task.period, task.utilisation, task.ex_time) for task in task_set]
            results = [task_list, total_utilisation, total_utilisation_perc/float(10)]
            lg.log_results("RTS 16 32", results)


    '''task_list = main(10,5,"random")
    for task in task_list:
        print (task.number, task.period, task.utilisation, task.ex_time)'''

