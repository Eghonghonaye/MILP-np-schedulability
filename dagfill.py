from backfill import overlap, conflicts

def backfill_order_criterion(j):
    # sort by later deadline, tie-break by later release, then by cost
    return (j.dag_deadline, j.dag_release, j.cost)

def select_next_job(jobs):
    return max(jobs, key=backfill_order_criterion, default=None)

def backfill_job(j, sched):
    # assumption: sched is a list of non-overlapping (job, start-time) tuples
    # let's look at only relevant jobs that overlap with j's feasibility window
    relevant = ((sj, start) for (sj, start) in sched
                if overlap((j.dag_release, j.dag_deadline), (start, start + sj.cost)))
    relevant = sorted(relevant, reverse=True, key=lambda x: x[1])
    # first, see if we can schedule the job just before its deadline (as late as possible)
    if j.dag_deadline - j.cost >= j.dag_release and \
       not conflicts(j.dag_deadline - j.cost, j.cost, relevant):
        alloc = (j, j.dag_deadline - j.cost)
        update_dag_constraints(alloc)
        sched.append(alloc)
        return True
    # nope, something's in the way
    # try before every other job that's potentially blocking us
    for sj, start in relevant:
        candidate = min(start, j.dag_deadline) - j.cost
        candidate = max(candidate, j.dag_release)
        if candidate + j.cost <= j.dag_deadline and \
           not conflicts(candidate, j.cost, relevant):
            alloc = (j, candidate)
            update_dag_constraints(alloc)
            sched.append(alloc)
            return True
    # nope, nothing worked
    return False

def backfill_first_fit(jobs, schedule):
    remaining = set(jobs)
    unassigned = set()
    while remaining:
        # look at the set of remaining jobs without unplaced successors
        candidates = (j for j in remaining
                      if not any((s in remaining for s in j.successors)))
        # select next job to consider
        j = select_next_job(candidates)
        if not j:
            # uh oh, couldn't assign the rest, no more candidates to try
            unassigned |= remaining
            break
        remaining.remove(j)
        success = False
        for core in schedule:
            if backfill_job(j, schedule[core]):
                success = True
                break
        if not success:
            unassigned.add(j)
    return (unassigned, schedule)

def prep_dag(jobs):
    def latest_finish(j):
        dl = j.deadline
        for s in j.successors:
            dl = min(dl, latest_finish(s) - s.cost)
        return dl

    def earliest_start(j):
        rel = j.release
        for p in j.predecessors:
            rel = max(rel, earliest_start(p) + p.cost)
        return rel

    # reduce feasibility window to account for successors
    for j in jobs:
        j.dag_deadline = latest_finish(j)
        j.dag_release  = earliest_start(j)

def update_dag_constraints(alloc):
    # update feasibility window of related jobs when job was placed
    j, start_time = alloc
    for p in j.predecessors:
        p.dag_deadline = min(p.dag_deadline, start_time)
    for s in j.successors:
        s.dag_release = max(s.dag_release, start_time + j.cost)

def paf_meta_heuristic(jobs, cores, heuristic=backfill_first_fit):
    difficult = set()
    regular   = set(jobs)
    give_up = False
    while not give_up:
        # first, create an empty schedule
        schedule = {}
        for core in range(cores):
            schedule[core] = []
        # prep the jobs
        prep_dag(jobs)
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
        if not unassigned2:
            # we found a feasible schedule!
            break
    return (unassigned1 | unassigned2,  schedule, difficult)

