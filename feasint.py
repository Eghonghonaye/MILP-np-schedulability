from backfill import overlap, conflicts

def init_feas(jobs, cores):
    for j in jobs:
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

def update_feas(jobs, core, scheduled_job, start_time):
    end_time = start_time + scheduled_job.cost

    for j in jobs:
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

def latest_startpoint(job):
    per_core = ((c, max(job.feasibility[c], key=lambda x: x[1]))
                for c in job.feasibility if job.feasibility[c])
    return max(per_core, key=lambda x: x[1][1], default=None)

def order_criterion(j):
    fcores, tfeas = feas_score(j)
    latest_pos = latest_startpoint(j)
    return (
        1/fcores if fcores > 0 else 1,
        latest_pos[1][1] if latest_pos else 0,
        1/tfeas if tfeas > 0 else 1,
        j.cost,
    )

def select_next_job(jobs):
    return max(jobs, key=order_criterion, default=None)

def backfill_latest_fit(jobs, schedule, all_jobs):
    remaining = set(jobs)
    unassigned = set()
    while remaining:
        j = select_next_job(remaining)
        remaining.remove(j)
        latest_pos = latest_startpoint(j)
        if latest_pos:
            core, (_, start_time) = latest_pos
            schedule[core].append((j, start_time))
            # reduce the feasibility windows of everyone else
            update_feas(all_jobs, core, j, start_time)
        else:
            unassigned.add(j)

    return (unassigned, schedule)


def paf_meta_heuristic(jobs, cores, heuristic=backfill_latest_fit):
    difficult = set()
    regular   = set(jobs)

    def difficult_succs(j):
        for s in j.successors:
            if not s in difficult:
                difficult.add(s)
                regular.remove(s)
                difficult_succs(s)

    give_up = False
    while not give_up:
        # first, create an empty schedule
        schedule = {}
        for core in range(cores):
            schedule[core] = []
        init_feas(jobs, cores)
        # pre-allocate the difficult ones
        (unassigned1, schedule) = heuristic(difficult, schedule, jobs)
        if unassigned1:
            # can't even pre-allocate, this is getting too difficult
            give_up = True
        # now try allocating the rest
        (unassigned2, schedule) = heuristic(regular, schedule, jobs)
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

