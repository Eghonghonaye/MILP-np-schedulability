import sys
import milpForm 
import parser
import result_logging as lg

def main():
	number_of_cores = int(sys.argv[1])
	number_of_tasks = int(sys.argv[2])
	for utilisation in [90,80,70,60,50,40,30,20,10]:
		path = "TaskSets/" + str(number_of_cores) + "Cores" + str(number_of_tasks) + "Tasks" + str(utilisation) + ".csv"
		resultPath = "MILPresults" + str(number_of_cores) + "Cores" + str(number_of_tasks) + "Tasks"

		for jobs, releaseTimes, deadlines, executionTimes, processors, M in parser.main(number_of_cores,path):
			status = milpForm.runExperiment(jobs, releaseTimes, deadlines, executionTimes, processors, M)
			lg.log_results(resultPath, [utilisation,status])

if __name__ == '__main__':
	main()