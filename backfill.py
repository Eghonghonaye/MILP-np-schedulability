
def backfill_order(jobs):
    return sorted((j for j in jobs),
                  reverse=True,
                  # sort by later deadline, tie-break by later release, then by cost
                  key=lambda j: (j.deadline, j.release, j.cost) )

def overlap(i1, i2):
    "does [a,b) overlap wiht [x, y)?"
    (a, b) = i1
    (x, y) = i2
    return a <= x < b or x <= a < y

def conflicts(proposed_start_time, cost, already_placed):
    return any((overlap((proposed_start_time, proposed_start_time + cost),
                        (start, start + sj.cost))
                for sj, start in already_placed))

def backfill_job(j, sched):
    # assumption: sched is a list of non-overlapping (job, start-time) tuples
    # let's look at only relevant jobs that overlap with j's feasibility window
    relevant = ((sj, start) for (sj, start) in sched
                if overlap((j.release, j.deadline), (start, start + sj.cost)))
    relevant = sorted(relevant, reverse=True, key=lambda x: x[1])
    # first, see if we can schedule the job just before its deadline (as late as possible)
    if not conflicts(j.deadline - j.cost, j.cost, relevant):
        sched.append((j, j.deadline - j.cost))
        return True
    # nope, something's in the way
    # try before every other job that's potentially blocking us
    for sj, start in relevant:
        candidate = min(start, j.deadline) - j.cost
        candidate = max(candidate, j.release)
        if candidate + j.cost <= j.deadline and \
           not conflicts(candidate, j.cost, relevant):
            sched.append((j, candidate))
            return True
    # nope, nothing worked
    return False


def backfill_first_fit(jobs, schedule):
    unassigned = set()
    for j in backfill_order(jobs):
        success = False
        for core in schedule:
            if backfill_job(j, schedule[core]):
                success = True
                break
        if not success:
            unassigned.add(j)
    return (unassigned, schedule)


def paf_meta_heuristic(jobs, cores, heuristic=backfill_first_fit):
    difficult = set()
    regular   = set(jobs)
    give_up = False
    while not give_up:
        # first, create an empty schedule
        schedule = {}
        for core in range(cores):
            schedule[core] = []
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

