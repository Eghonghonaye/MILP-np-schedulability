from backfill import overlap, conflicts

from order import ConsiderationOrder

def backfill_order_criterion(j):
    # sort by later deadline, tie-break by later release, then by cost
    return (
        j.succ_count,
        -j.dag_deadline,
        -j.dag_release,
        -j.cost,
        j.id, # make sure there are no ties
    )

def backfill_job(j, sched, queue):
    # assumption: sched is a list of non-overlapping (job, start-time) tuples
    # let's look at only relevant jobs that overlap with j's feasibility window
    relevant = ((sj, start) for (sj, start) in sched
                if overlap((j.dag_release, j.dag_deadline), (start, start + sj.cost)))
    relevant = sorted(relevant, reverse=True, key=lambda x: x[1])
    # first, see if we can schedule the job just before its deadline (as late as possible)
    if j.dag_deadline - j.cost >= j.dag_release and \
       not conflicts(j.dag_deadline - j.cost, j.cost, relevant):
        alloc = (j, j.dag_deadline - j.cost)
        update_dag_constraints(alloc, queue)
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
            update_dag_constraints(alloc, queue)
            sched.append(alloc)
            return True
    # nope, nothing worked
    return False

def backfill_first_fit(jobs, schedule):
    unassigned = set()
    queue = ConsiderationOrder(backfill_order_criterion, jobs)

    while True:
        # select next job to consider
        j = queue.next()
        if not j:
            break
        success = False
        for core in schedule:
            if backfill_job(j, schedule[core], queue):
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

    for j in jobs:
        j.succ_count = len(j.successors)


def update_dag_constraints(alloc, queue):
    # update feasibility window of related jobs when job was placed
    j, start_time = alloc
    for p in j.predecessors:
        if p.in_queue:
            p.dag_deadline = min(p.dag_deadline, start_time)
            p.succ_count -= 1
            queue.update(p)
    for s in j.successors:
        if s.in_queue:
            s.dag_release = max(s.dag_release, start_time + j.cost)
            queue.update(s)

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

