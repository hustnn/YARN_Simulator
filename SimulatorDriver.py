'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Resource import Resource 
from File import File
from Cluster import Cluster
from YARNScheduler import YARNScheduler
from WorkloadGenerator import WorkloadGenerator
from SimilarityType import SimilarityType

import Configuration
import JobFileGenerator

import time


def getFairnessStatistic(currentResult, baselineResult):
    overallFairness = 0
    unfairness = 0
    relativeAppFairness = {}
    
    for appID, finishedTime in currentResult.items():
        relativeAppFairness[appID] = baselineResult[appID] - finishedTime
        
    for v in relativeAppFairness.values():
        overallFairness += v
        if v < 0:
            unfairness += v
            
    return overallFairness, unfairness, relativeAppFairness
            
# tradeoff factor = 1 means fair
def execSimulation(workloadFile, tradeoffFactor, clusterSize = 10):
    cluster = Cluster(clusterSize)
    fileList = [{"name": "wiki-40G", "size": Configuration.BLOCK_SIZE * 4 * 10}]
    for f in fileList:
        hadoopFile = File(f["name"], f["size"])
        cluster.uploadFile(hadoopFile)
        
    scheduler = YARNScheduler(cluster, True, tradeoffFactor)
    
    scheduler.createQueue("queue1", "PACKING", True, "root")
    
    #queueWorkloads = {"queue1": "q1-workload"}
    queueWorkloads = {"queue1": workloadFile}
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, queueWorkloads, cluster)
    
    '''for k in queueWorkloads.keys():
        v = workloadGen.getQueues()[k]
        for job in v["jobList"]:
            print(job.getSubmissionTime(), len(job.getTaskList()))'''
        
    simulationStepCount = 0
    
    while True:
        #print(workloadGen.allJobsSubmitted(), len(scheduler.getAllApplications()))
        if workloadGen.allJobsSubmitted() and len(scheduler.getAllApplications()) == 0:
            break
        
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        #print("currentTime: " + str(currentTime))
        workloadGen.submitJobs(currentTime, scheduler)
        scheduler.simulate(Configuration.SIMULATION_STEP, currentTime)
        simulationStepCount += 1
        
        #time.sleep(1)
        #print("\n")
        
    #for app in scheduler.getFinishedApps():
    #    print(app.getApplicationID(), app.getJob().getFinishTime())
        
    #for node in scheduler.getAllNodes():
    #    print(str(node.getAvailableResource()))
    
    makespan = simulationStepCount * Configuration.SIMULATION_STEP
    #print("simulation end: " + str(makespan))
    
    finishedApp = scheduler.getFinishedAppsInfo()
    
    return makespan, finishedApp

if __name__ == '__main__':
    '''print("all same and mixed combination")
    workloadList = ["allL-allS", "allL-mixed", "allS-mixed", "mixed-mixed"]
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("all resource type combination")
    workloadList = ["oneType", "twoType", "threeType", "fourType"]
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("all dominant resource combination")
    workloadList = ["oneDominant", "twoDominant", "threeDominant", "fourDominant"]
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    #all same and mixed varying resource capacity
    print("allL and allS varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("allL-allS" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("allL and allS varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("allL-allS" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("allL and mixed varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("allL-mixed" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("allL and mixed varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("allL-mixed" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("allS and mixed varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("allS-mixed" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("allS and mixed varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("allS-mixed" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("mixed and mixed varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("mixed-mixed" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("mixed and mixed varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("mixed-mixed" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    # resource type varying resource capacity
    print("one resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("oneType" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("two resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("twoType" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("three resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("threeType" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("four resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("fourType" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("one resource type varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("oneType" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("two resource type varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("twoType" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("three resource type varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("threeType" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("four resource type varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("fourType" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    #dominant resource varying resource capacity
    print("one dominant resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("oneDominant" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("two dominant resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("twoDominant" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("three dominant resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("threeDominant" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("four dominant resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("fourDominant" + "-L" + str(index) + "-S" + str(0))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("one dominant resource type varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("oneDominant" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("two dominant resource type varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("twoDominant" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("three dominant resource type varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("threeDominant" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("four dominant resource type varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("fourDominant" + "-L" + str(5) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    # varying workload load by adjust cluster size
    print("mixed and mixed varying cluster size")
    workload = "mixed-mixed"
    for size in [1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1, size)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0, size)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "-" + str(size) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("mixed and mixed varying cluster size")
    workload = "mixed-mixed-L4-S0"
    for size in [1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1, size)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0, size)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "-" + str(size) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")'''
    
    '''
    print("mixed and mixed varying job size of memory dominant job")
    for jobSize in [10, 20, 40, 60, 80, 100]:
        workload = "mixed-mixed-LJ" + str(jobSize)
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("mixed and mixed varying task size of memory dominant job")
    for taskSize in [5, 10, 15, 20, 25, 30]:
        workload = "mixed-mixed-LT" + str(taskSize)
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")'''
    
    '''print("allL and allS varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("allL-allS" + "-L" + str(4) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")'''
    
    '''
    print("one resource type varying S")
    workloadList = []
    for index in range(5):
        workloadList.append("oneType" + "-L" + str(4) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("two resource type varying S")
    workloadList = []
    for index in range(7):
        workloadList.append("twoType" + "-L" + str(6) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("three resource type varying S")
    workloadList = []
    for index in range(4):
        workloadList.append("threeType" + "-L" + str(3) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")'''
    
    print("two dominant resource type varying S")
    workloadList = []
    largeIndex = 6
    for index in range(largeIndex + 1):
        workloadList.append("twoDominant" + "-L" + str(largeIndex) + "-S" + str(index))
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        #print(workload, fairMakespan, 0, perfMakespan, -1 * unfairness)
        print(workload + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
        