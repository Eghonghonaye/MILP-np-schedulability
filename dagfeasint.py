from itertools import combinations

from backfill import overlap, conflicts
from order import ConsiderationOrder

def init_feas(jobs, cores):
    for j in jobs:
        j.succ_count = len(j.successors)
        j.feasibility = {}
        j.feas_cores = cores
        j.feas_region = (j.deadline - j.cost - j.release) * cores
        for core in range(cores):
            j.feasibility[core] = [[j.release, j.deadline - j.cost]]


def update_feas(core, scheduled_job, start_time, queue, later_jobs):
    end_time = start_time + scheduled_job.cost

    for j in scheduled_job.overlapping_jobs:
        if j.in_queue or j in later_jobs:
            updated = False
            deleted = 0
            blocked = [start_time - j.cost, end_time]
            for i in range(len(j.feasibility[core])):
                region = j.feasibility[core][i - deleted]
                a, b = region
                if a < blocked[0] and blocked[1] < b:
                    # falls squarely in the middle
                    # split in two parts
                    # update first
                    region[1] = blocked[0]
                    # add new for second
                    j.feasibility[core].append([blocked[1], b])
                    j.feas_region -= blocked[1] - blocked[0]
                    updated = True
                elif a <= blocked[0] and b <= blocked[1]:
                    # possible overlap at the end
                    b2 = min(b, blocked[0])
                    if b2 < b:
                        updated = True
                        region[1] = b2
                        j.feas_region -= b - b2
                elif a <= blocked[1] <= b:
                    # possible overlap at the beginning
                    a2 = max(a, blocked[1])
                    if a2 > a:
                        updated = True
                        region[0] = a2
                        j.feas_region -= a2 - a
                elif blocked[0] < a and b < blocked[1]:
                    # completely covered => infeasible, remove
                    del j.feasibility[core][i - deleted]
                    deleted += 1
                    j.feas_region -= b - a
                    updated = True
                else:
                    # not overlapping, nothing to do
                    pass
            # check wether we lost a core
            if deleted and not j.feasibility[core]:
                j.feas_cores -= 1
            if updated and j.in_queue:
                queue.update(j)

def update_dag_constraints(j, start_time, queue, later_jobs):
    end_time = start_time + j.cost
    for p in j.predecessors:
        if p.in_queue or j in later_jobs:
            p.succ_count -= 1
            for c in p.feasibility:
                updated = ([a, min(b, start_time - p.cost)] for a, b in p.feasibility[c])
                p.feasibility[c] = [[a, b] for a, b in updated if a <= b]
            p.feas_cores = sum((1 for c in p.feasibility if p.feasibility[c]))
            p.feas_region = sum( (sum((b - a for (a, b) in p.feasibility[c]))
                                  for c in p.feasibility) )
            if p.in_queue:
                queue.update(p)
    for s in j.successors:
        if s.in_queue or j in later_jobs:
            for c in s.feasibility:
                updated = ([max(a, end_time), b] for (a, b) in s.feasibility[c])
                s.feasibility[c] = [[a, b] for a, b in updated if a <= b]
            s.feas_cores = sum((1 for c in s.feasibility if s.feasibility[c]))
            s.feas_region = sum( (sum((b - a for (a, b) in s.feasibility[c]))
                                  for c in s.feasibility) )
            if s.in_queue:
                queue.update(s)

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
    latest_pos = latest_startpoint(j)
    return (
        j.succ_count,
        j.feas_cores,
        -latest_pos[1][1] if latest_pos else 0,
        j.feas_region,
        -j.cost,
    )

def backfill_latest_fit(jobs, schedule, later_jobs=set()):
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
            # update the feasibility windows of predecessors and successors
            update_dag_constraints(j, start_time, queue, later_jobs)
            # reduce the feasibility windows of everyone else
            update_feas(core, j, start_time, queue, later_jobs)
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
        (unassigned1, schedule) = heuristic(difficult, schedule, regular)
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

