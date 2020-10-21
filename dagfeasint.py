from itertools import combinations

from backfill import overlap, conflicts
from order import ConsiderationOrder


def init_feas(jobs, cores):
    for j in jobs:
        j.succ_count = len(j.successors)
        j.feasibility = {}
        for core in range(cores):
            j.feasibility[core] = [(j.release, j.deadline - j.cost)]

def feas_score(j):
    total = 0
    for core in j.feasibility:
        for (a, b) in  j.feasibility[core]:
            assert b >= a
            total += b - a + 1
    feasible_cores = sum((1 for c in j.feasibility if j.feasibility[c]))
    total_region = sum( (sum((b - a + 1 for (a, b) in j.feasibility[c]))
                         for c in j.feasibility) )
    return feasible_cores, total_region

def update_feas(core, scheduled_job, start_time, queue):
    end_time = start_time + scheduled_job.cost

    for j in scheduled_job.overlapping_jobs:
        if j.in_queue:
            updated = []
            for (a, b) in j.feasibility[core]:
                if a <= start_time and b <= end_time:
                    # possible overlap at the end
                    updated.append((a, min(b, start_time - j.cost)))
                elif start_time <= a and end_time <= b:
                    # possible overlap at the beginning
                    updated.append((max(a, end_time), b))
                elif start_time <= a and b <= end_time:
                    # completely covered => infeasible, nothing to add
                    pass
                elif a <= start_time <= end_time <= b:
                    # possible split in two intervals
                    updated.append((a, start_time - j.cost))
                    updated.append((end_time, b))
                else:
                    assert False
            j.feasibility[core] = [(a, b) for (a, b) in updated if a <= b]
            queue.update(j)

def latest_startpoint(job):
    per_core = ((c, max(job.feasibility[c], key=lambda x: x[1]))
                for c in job.feasibility if job.feasibility[c])
    return max(per_core, key=lambda x: x[1][1], default=None)

def init_overlap(jobs):
    for j in jobs:
        j.overlapping_jobs = []
    for j1, j2 in combinations(jobs, 2):
        feas1 = (j1.release, j1.deadline)
        feas2 = (j2.release, j2.deadline)
        if overlap(feas1, feas2):
            j1.overlapping_jobs.append(j2)
            j2.overlapping_jobs.append(j1)

def order_criterion(j):
    fcores, tfeas = feas_score(j)
    latest_pos = latest_startpoint(j)
    return (
        j.succ_count,
        fcores,
        -latest_pos[1][1] if latest_pos else 0,
        tfeas,
        -j.cost,
    )

def update_dag_constraints(j, start_time, queue):
    end_time = start_time + j.cost
    for p in j.predecessors:
        if p.in_queue:
            p.succ_count -= 1
            for c in p.feasibility:
                updated = ((a, min(b, start_time - p.cost)) for (a, b) in p.feasibility[c])
                p.feasibility[c] = [(a, b) for (a, b) in updated if a <= b]
            queue.update(p)
    for s in j.successors:
        if s.in_queue:
            for c in s.feasibility:
                updated = ((max(a, end_time), b) for (a, b) in s.feasibility[c])
                s.feasibility[c] = [(a, b) for (a, b) in updated if a <= b]
            queue.update(s)

def backfill_latest_fit(jobs, schedule):
    unassigned = set()
    queue = ConsiderationOrder(order_criterion, jobs)

    while True:
        # select next job to consider
        j = queue.next()
        if not j:
            break
        latest_pos = latest_startpoint(j)
        if latest_pos:
            core, (_, start_time) = latest_pos
            schedule[core].append((j, start_time))
            # reduce the feasibility windows of everyone else
            update_feas(core, j, start_time, queue)
            # update the feasibility windows of predecessors and successors
            update_dag_constraints(j, start_time, queue)
        else:
            unassigned.add(j)

    return (unassigned, schedule)

def paf_meta_heuristic(jobs, cores, heuristic=backfill_latest_fit):
    difficult = set()
    regular   = set(jobs)
    give_up = False

    def difficult_succs(j):
        for s in j.successors:
            if not s in difficult:
                difficult.add(s)
                regular.remove(s)
                difficult_succs(s)

    init_overlap(jobs)

    while not give_up:
        # first, create an empty schedule
        schedule = {}
        for core in range(cores):
            schedule[core] = []
        init_feas(jobs, cores)
        # pre-allocate the difficult ones
        (unassigned1, schedule) = heuristic(difficult, schedule)
        if unassigned1:
            # can't even pre-allocate, this is getting too difficult
            give_up = True
        # now try allocating the rest
        (unassigned2, schedule) = heuristic(regular, schedule)
        # see if we found anything new that's difficult
        difficult |= unassigned2
        regular   -= unassigned2
        # make sure we get all the successors, too, rather than discovering them slowly
        for j in unassigned2:
            difficult_succs(j)
        if not unassigned2:
            # we found a feasible schedule!
            break
    return (unassigned1 | unassigned2,  schedule, difficult)

