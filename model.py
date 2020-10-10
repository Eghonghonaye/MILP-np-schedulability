from gurobipy import *
from itertools import combinations

def make_gurobi_milp(releaseTimes, deadlines, executionTimes, ncores, M, name='RAP'):
    # declare and init model
    m = Model(name)

    njobs = len(releaseTimes)
    assert len(deadlines) == njobs
    assert len(executionTimes) == njobs

    #decision variables
    x = m.addVars(njobs, ncores, vtype=GRB.BINARY, name = "assign")
    s = m.addVars(njobs, name = "startTime")

    # Auxiliary variables
    f = m.addVars(njobs, name = "finishTime")

    # define finish times
    m.addConstrs((f[i] == s[i] + executionTimes[i] for i in range(njobs)),
                  'jobfinish')

    # problem constraints
    assignment = m.addConstrs(((x.sum(j,'*')) == 1 for j in range(njobs)), 'jobassign')
    starting = m.addConstrs((s[i] >= releaseTimes[i] for i in range(njobs)), 'jobstart')
    deadline = m.addConstrs((f[i] <= deadlines[i] for i in range(njobs)), 'jobdeadline')

    # define max/min helpers, but only for the cases where it matters

    def relevant(i, j):
        # we don't care about jobs that cannot overlap by def. of their feasibility intervals
        return i != j and \
               not (releaseTimes[i] >= deadlines[j] or
                    releaseTimes[j] >= deadlines[i])

    for seqno, (i, j) in enumerate(combinations(range(njobs), 2)):
        if relevant(i, j):
            min_start = m.addVar(name = 'minStart[%d,%d]' % (i, j))
            max_fin   = m.addVar(name = 'maxStart[%d,%d]' % (i, j))

            # define minimum of start times
            m.addConstr(min_start == min_(s[i], s[j]))
            # define maximum of finish times
            m.addConstr(max_fin == max_(f[i], f[j]))

            # add non-overlap constraints on each core
            m.addConstrs((min_start + executionTimes[i] + executionTimes[j]
                          <= max_fin + M * (1 - x[i,k]) + M * (1 - x[j,k])
                          for k in range(ncores)))

    m.setObjective(0)

    return m
