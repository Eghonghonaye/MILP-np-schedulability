from math import ceil, floor

def decompose_limited_preemptive(jobs, integral=True):
    "resolve precedence constraints of jobs by tweaking release/deadlines"

    def preceding_linear_work(j):
        prior_cost = 0
        while j.predecessors:
            assert len(j.predecessors) == 1 # assumes linear chains of jobs
            j = j.predecessors[0]
            prior_cost += j.cost
        return prior_cost

    def remaining_linear_work(j):
        remaining_cost = 0
        while j.successors:
            assert len(j.successors) == 1 # assumes linear chains of jobs
            j = j.successors[0]
            remaining_cost += j.cost
        return remaining_cost

    for j in jobs:
        j.decomp_release  = j.release
        j.decomp_deadline = j.deadline

        pcost = preceding_linear_work(j)
        rcost = remaining_linear_work(j)

        if pcost == 0 and rcost == 0:
            continue # not a DAG job

        total = pcost + rcost + j.cost

        interval = j.deadline - j.release

        pre = ((pcost / total) * interval)
        rem = ((rcost / total) * interval)
        if integral:
           pre = floor(pre)
           rem = ceil(rem)

        # tweak release and deadline
        j.release  += pre
        j.deadline -= rem
        print(j.id, j.decomp_release, j.decomp_deadline, j.release, j.deadline)

    # now mask DAG structure
    for j in jobs:
        j.decomp_pred  = j.predecessors
        j.predecessors = []
        j.decomp_succ  = j.successors
        j.successors   = []


def decompose_restore(jobs):
    # restore original parameters
    for j in jobs:
        j.predecessors = j.decomp_pred
        j.successors   = j.decomp_succ
        j.release      = j.decomp_release
        j.deadline     = j.decomp_deadline
