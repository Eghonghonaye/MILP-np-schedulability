import csv, task_generator
import ast

def main(path="/home/eghonghonaye/Desktop/tasks.csv"):
	task_list = []
	all_task_sets = []
	with open(path) as csv_file:
	    csv_reader = csv.reader(csv_file, delimiter=',')
	    for row in csv_reader:
	    	task_set = ast.literal_eval(row[0])
	    	#print(task_set)
	    	for task in task_set:
	    		number = task[0]
	    		period = task[1]
	    		util = task[2]
	    		task_list.append(task_generator.generateTask(number, period, util))#, 15.85955591030769)
	    	all_task_sets.append(task_list)
	    	task_list = []
	return (all_task_sets)

#print(all_task_sets)

