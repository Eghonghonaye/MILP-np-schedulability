from backfill import overlap, conflicts

from feasint import init_feas, select_next_job, update_feas, latest_startpoint

def update_dag_constraints(j, start_time):
    end_time = start_time + j.cost
    for p in j.predecessors:
        for c in p.feasibility:
            updated = ((a, min(b, start_time - p.cost)) for (a, b) in p.feasibility[c])
            p.feasibility[c] = [(a, b) for (a, b) in updated if a <= b]
    for s in j.successors:
        for c in s.feasibility:
            updated = ((max(a, end_time), b) for (a, b) in s.feasibility[c])
            s.feasibility[c] = [(a, b) for (a, b) in updated if a <= b]

def backfill_latest_fit(jobs, schedule, all_jobs):
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
        latest_pos = latest_startpoint(j)
        if latest_pos:
            core, (_, start_time) = latest_pos
            schedule[core].append((j, start_time))
            # reduce the feasibility windows of everyone else
            update_feas(all_jobs, core, j, start_time)
            # update the feasibility windows of predecessors and successors
            update_dag_constraints(j, start_time)
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

