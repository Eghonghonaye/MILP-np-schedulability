# MILP formulation of non-preemptive scheduling problem
The tool takes a set of tasks and returns the key gotten from the gurobi solver for that problem.

## Input
The input is the collection of task sets saved in the TaskSet folder. They are for tasks on 4 and 8 cores with number of tasks in the set {2m, 3m, 4m} where m is the nmber of cores.

## Output
The tool outputs a code for each task set and saves it to a csv file

## How to run
python3 runExp.py $numberOfCores $numberOfTasks

The numbers for which task sets have been generated are: {(4,8),(4,12),(4,16),(8,16),(8,24),(8,32)}

## Dependencies
Gurobi Optimizer (Python API) https://www.gurobi.com/products/gurobi-optimizer/
