from gurobipy import *
from itertools import combinations 
from itertools import permutations 


'''gurobi status codes
Status code	Value	Description
LOADED	1	Model is loaded, but no solution information is available.
OPTIMAL	2	Model was solved to optimality (subject to tolerances), and an optimal solution is available.
INFEASIBLE	3	Model was proven to be infeasible.
INF_OR_UNBD	4	Model was proven to be either infeasible or unbounded. To obtain a more definitive conclusion, set the DualReductions parameter to 0 and reoptimize.
UNBOUNDED	5	Model was proven to be unbounded. Important note: an unbounded status indicates the presence of an unbounded ray that allows the objective to improve without limit. It says nothing about whether the model has a feasible solution. If you require information on feasibility, you should set the objective to zero and reoptimize.
CUTOFF	6	Optimal objective for model was proven to be worse than the value specified in the Cutoff parameter. No solution information is available.
ITERATION_LIMIT	7	Optimization terminated because the total number of simplex iterations performed exceeded the value specified in the IterationLimit parameter, or because the total number of barrier iterations exceeded the value specified in the BarIterLimit parameter.
NODE_LIMIT	8	Optimization terminated because the total number of branch-and-cut nodes explored exceeded the value specified in the NodeLimit parameter.
TIME_LIMIT	9	Optimization terminated because the time expended exceeded the value specified in the TimeLimit parameter.
SOLUTION_LIMIT	10	Optimization terminated because the number of solutions found reached the value specified in the SolutionLimit parameter.
INTERRUPTED	11	Optimization was terminated by the user.
NUMERIC	12	Optimization was terminated due to unrecoverable numerical difficulties.
SUBOPTIMAL	13	Unable to satisfy optimality tolerances; a sub-optimal solution is available.
INPROGRESS	14	An asynchronous optimization call was made, but the associated optimization run is not yet complete.
USER_OBJ_LIMIT	15	User specified an objective limit (a bound on either the best objective or the best bound), and that limit has been reached.
'''


def runModel(jobs, releaseTimes, deadlines, executionTimes, processors, M):
	#declare and init model
	m = Model('RAP')

	#decision variables
	x = m.addVars(len(jobs), len(processors), vtype=GRB.BINARY, name = "assign")
	s = m.addVars(len(jobs), name = "startTime")
	theta = m.addVars(len(jobs), len(jobs), vtype=GRB.BINARY, name = "overlap")

	#constraints
	assignment = m.addConstrs(((x.sum(j,'*')) == 1 for j in range(len(jobs))),'jobassign')
	starting = m.addConstrs((s[i] >= releaseTimes[i] for i in range(len(jobs))),'jobstart')
	deadline = m.addConstrs((s[i] + executionTimes[i]*x.sum(i,'*') <= deadlines[i] for i in range(len(jobs))),'jobdeadline')

	#jobs must not ovrlap on the same processor
	overlapping1 = m.addConstrs((
		x[i,k] + x[j,k] + theta[i,j] + theta[j,i] <= 3 	
		for (l,m) in combinations([job for job in range(len(jobs))], 2) 
		for (i,j) in permutations([l,m]) 
		for k in range(len(processors))
		if i != j
		), 'joboverlap')

	overlapping2 = m.addConstrs((s[i] - s[j] - executionTimes[j]*x.sum(j,'*') >= -M*theta[j,i] 
		for (l,m) in combinations([job for job in range(len(jobs))], 2)
		for (i,j) in permutations([l,m]) 
		if i != j
		), 'joboverlap2')

	overlapping3 = m.addConstrs((s[i] - s[j] - executionTimes[j]*x.sum(j,'*') <= M*(1-theta[j,i]) 
		for (l,m) in combinations([job for job in range(len(jobs))], 2)
		for (i,j) in permutations([l,m]) 
		if i != j
		), 'joboverlap3')
	


	#assume no objective function

	#save model
	#m.write('Sched.lp')

	m.optimize()
	return m.status

def runExperiment(jobs, releaseTimes, deadlines, executionTimes, processors, M):
	try:
		status = runModel(jobs, releaseTimes, deadlines, executionTimes, processors, M) 
		return status		
	except gurobipy.GurobiError as e:
		return 'Error code ' + str(e.errno) + ': ' + str(e)
	except MemoryError:
		return "memory"
	except:
		return "failed"

if __name__ == '__main__':
	pass


