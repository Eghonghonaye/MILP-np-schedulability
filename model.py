from gurobipy import *
from itertools import combinations


def demand_of_job(release, cost, deadline, a, b):
    "demand of a job in the interval [a, b)"
    pre  = max(0, a - release)  # time before a after release
    post = max(0, deadline - b) # time after  b after deadline
    return max(0, cost - pre - post)

def demand_intervals_before(b, releases, deadlines, costs):
    work = 0
    # for efficiency reasons, this is doing an intertwined double iteration
    jobs = iter(sorted(range(len(releases)), key=lambda i: releases[i], reverse=True))
    points = iter(sorted(set((r for r in releases if r < b)), reverse = True))
    cur_job = next(jobs)
    a = next(points)
    while True:
        # count work of relevant jobs
        if releases[cur_job] >= a and deadlines[cur_job] <= b:
            work += costs[cur_job]
        # we move from later to earlier releases, so if we hit this condition,
        # we're done with a
        if releases[cur_job] < a:
            yield (work / (b - a), a)
            # advance point
            a = next(points)
        else:
            # advance job
            try:
                cur_job = next(jobs)
            except StopIteration:
                # ran out of jobs
                # yield current point and then stop
                yield (work / (b - a), a)
                break

def max_demand_interval_before(b, releases, deadlines, costs):
    demand_ratio, a = max(demand_intervals_before(b, releases, deadlines, costs),
                          default=0)
    return demand_ratio, (a, b)

def intervals_of_interest(releases, deadlines, costs):
    return ((a, b) for ratio, (a, b) in
                (max_demand_interval_before(d, releases, deadlines, costs)
                 for d in set(deadlines))
            if ratio > 1)

def make_gurobi_milp(releaseTimes, deadlines, executionTimes, ncores, M, name='RAP',
                     with_demand_constraints=False):
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

    for i, j in combinations(range(njobs), 2):
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

    # Generate demand constraints --- these are strictly speaking redundant,
    # but serve to guide the solver.
    def demand_constraint(a, b, k):
        coeffs = [demand_of_job(releaseTimes[i], executionTimes[i], deadlines[i], a, b)
                  for i in range(njobs) if releaseTimes[i] < b and deadlines[i] > a]
        vars   = [x[i,k] for i in range(njobs) if releaseTimes[i] < b and deadlines[i] > a]
        return LinExpr(coeffs, vars) <= (b - a)

    if with_demand_constraints:
        for (a, b) in intervals_of_interest(releaseTimes, deadlines, executionTimes):
            m.addConstrs((demand_constraint(a, b, k) for k in range(ncores)),
                          'demand-%d-%d' % (a, b))

    # we just want a feasible solution
    m.setObjective(0)

    return m
