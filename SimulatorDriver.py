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
    
    #overallfairness: including benefit and loss
    #unfairness: only include loss
    return overallFairness, unfairness, relativeAppFairness
            
# tradeoff factor = 1 means fair
def execSimulation(workloadFile, tradeoffFactor, clusterSize = 10, load = 0.0, similarityType = SimilarityType.PRODUCT, schedulingMode = "default", 
                   randomFactor = 0, batchSize = 20, vectorQuantinationNum = 20, entropy = 0.0):
    cluster = Cluster(clusterSize, load)
    fileList = [{"name": "wiki-40G", "size": Configuration.BLOCK_SIZE * 4 * 10}]
    for f in fileList:
        hadoopFile = File(f["name"], f["size"])
        cluster.uploadFile(hadoopFile)
        
    scheduler = YARNScheduler(cluster, True, tradeoffFactor, similarityType, schedulingMode, 
                              randomFactor, batchSize, vectorQuantinationNum, entropy)
    
    scheduler.createQueue("queue1", "MULTIFAIR", True, "root")
    
    #queueWorkloads = {"queue1": "q1-workload"}
    queueWorkloads = {"queue1": workloadFile}
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, queueWorkloads, cluster)
    for queue in workloadGen.getQueues().keys():
        workloadGen.genWorkload(queue)
    
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
    
    # resource utilization
    memory, cpu, disk, network = scheduler.getUtilization()
    #print(float(sum(memory)) / len(memory), float(sum(cpu)) / len(cpu), float(sum(disk)) / len(disk), float(sum(network)) / len(network))
    
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
    print("\n")
    
    #job,task size
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
    
    # actual makespan and relative improvement
    
    '''print("all same and mixed combination")
    workloadList = ["allL-allS", "allL-mixed", "allS-mixed", "mixed-mixed"]
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(workload + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("different resource types")
    workloadList = ["oneType", "twoType", "threeType", "fourType"]
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(workload + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("different number of dominant resources")
    workloadList = ["oneDominant", "twoDominant", "threeDominant", "fourDominant"]
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for workload in workloadList:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(workload + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("mixed and mixed varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("mixed-mixed" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("mixed and mixed varying S")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH / 2):
        workloadList.append("mixed-mixed" + "-L" + str(5) + "-S" + str(index))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("one resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("oneType" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("two resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("twoType" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("three resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("threeType" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("four resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("fourType" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("one dominant resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("oneDominant" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("two dominant resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("twoDominant" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("three dominant resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("threeDominant" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("four dominant resource type varying L")
    workloadList = []
    for index in range(JobFileGenerator.TOTAL_INDEX_LENGTH):
        workloadList.append("fourDominant" + "-L" + str(index) + "-S" + str(0))
    print("combination,fair makespan,perf makespan,reduction,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")
    
    print("mixed and mixed varying cluster load")
    workload = "mixed-mixed"
    print("load,fair makespan,perf makespan,reduction,unfairness")
    for load in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]:
        fairMakespan, fairFinishedApp = execSimulation(workload, 1, 10, load)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0, 10, load)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(load) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")'''
    
    #debug
    '''print("three resource type varying L")
    workload = "threeType-L10-S0"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print(str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))
    print("\n")'''
    
    # finding1
    '''workload = "finding1"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print(str(fairMakespan) + "," + str(perfMakespan) + "," + str(float(perfMakespan) / fairMakespan) + "," + str(-1 * unfairness))'''
    
    '''workload = "finding1"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    randMakespan, randFinishedApp = execSimulation(workload, 1, 10, 0.0, SimilarityType.PRODUCT, "random", 100)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(randFinishedApp, fairFinishedApp)
    print(str(fairMakespan) + "," + str(randMakespan) + "," + str(-1 * unfairness))'''
    
    '''workload = "finding1-random"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    #perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    #overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    #print(str(fairMakespan) + "," + str(perfMakespan) + "," + str(-1 * unfairness))
    #knobs = [0.98, 0.95, 0.92, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
    #for knob in knobs:
    for i in range(10):
    #    print("knob: " + str(knob))
        #randMakespan, randFinishedApp = execSimulation(workload, knob, 10, 0.0, SimilarityType.PRODUCT, "random")
        randMakespan, randFinishedApp = execSimulation(workload, 1, 10, 0.0, SimilarityType.PRODUCT, "random")
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(randFinishedApp, fairFinishedApp)
        print(str(fairMakespan) + "," + str(randMakespan) + "," + str(-1 * unfairness))'''
    
    # finding 2
    '''workload = "finding2-perf-small"
    print(workload)
    knobs = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    for knob in knobs:
        perfMakespan, perfFinishedApp = execSimulation(workload, knob)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(knob) + "," + str(perfMakespan) + "," + str(-1 * unfairness))'''
    
    '''workload = "finding2-perf-large"
    print(workload)
    knobs = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    for knob in knobs:
        perfMakespan, perfFinishedApp = execSimulation(workload, knob)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(knob) + "," + str(perfMakespan) + "," + str(-1 * unfairness))'''
        
        
    # finding 3
    '''workload = "finding3-random"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print(str(fairMakespan) + "," + str(perfMakespan) + "," + str(-1 * unfairness))
    #probList = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    probList = [45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95]
    for prob in probList:
        print("prob: " + str(prob))
        for i in range(3):
            randFairMakespan, randFairFinishedApp = execSimulation(workload, 1, 10, 0.0, SimilarityType.PRODUCT, "random", prob)
            overallFairness, fairUnfairness, relativeAppFairness = getFairnessStatistic(randFairFinishedApp, fairFinishedApp)
            randPerfMakespan, randPerfFinishedApp = execSimulation(workload, 0, 10, 0.0, SimilarityType.PRODUCT, "random", prob)
            overallFairness, perfUnfairness, relativeAppFairness = getFairnessStatistic(randPerfFinishedApp, fairFinishedApp)
            print(str(randFairMakespan) + "," + str(-1 * fairUnfairness) + "," + str(randPerfMakespan) + "," + str(-1 * perfUnfairness))'''
    
    # finding 4
    '''print("mixed and mixed varying S")
    workloadList = []
    for index in range(5):
        workloadList.append("finding4" + "-L" + str(5) + "-S" + str(index))
    print("combination,fair makespan,perf makespan,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(-1 * unfairness))
    print("\n")'''
        
    '''workloadList = ["finding4-memory", "finding4-memorycpu", "finding4-memorycpudisk", "finding4-memorycpudisknetwork"]
    print("combination,fair makespan,perf makespan,unfairness")
    for index in range(len(workloadList)):
        workload = workloadList[index]
        fairMakespan, fairFinishedApp = execSimulation(workload, 1)
        perfMakespan, perfFinishedApp = execSimulation(workload, 0)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
        print(str(index + 1) + "," + str(fairMakespan) + "," + str(perfMakespan) + "," + str(-1 * unfairness))
    print("\n")'''
    
    
    '''execSimulation(workloadFile, tradeoffFactor, clusterSize = 10, load = 0.0, similarityType = SimilarityType.PRODUCT, schedulingMode = "default", 
                   randomFactor = 0, batchSize = 20, vectorQuantinationNum = 20, entropy = 2.0):'''
    # experiments of ICPP paper
    '''workload = "micro-workload"
    print("fair begin")
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    print("fair: " + str(fairMakespan))
    
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print("perf: " + str(perfMakespan) + ", " + str(unfairness))'''
    
    '''print("batch size study")
    for batchSize in [20, 30, 40, 50, 60]:
        batchMakespan, batchFinishedApp = execSimulation(workload, 0, 10, 0.0, SimilarityType.PRODUCT, "batch", 0, batchSize, 10, 1)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(batchFinishedApp, fairFinishedApp)
        print("batch size: " + str(batchSize) + ", " + str(batchMakespan) + ", " + str(unfairness))'''
    
    '''print("vector quantination num study")
    for vqNum in [5, 10, 15, 20]:
        batchMakespan, batchFinishedApp = execSimulation(workload, 0, 10, 0.0, SimilarityType.PRODUCT, "batch", 0, 30, vqNum, 1.5)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(batchFinishedApp, fairFinishedApp)
        print("vq num: " + str(vqNum) + ", " + str(batchMakespan) + ", " + str(unfairness))'''
        
    '''print("entropy threshold study")
    for entropy in [1.0]:
        batchMakespan, batchFinishedApp = execSimulation(workload, 0, 10, 0.0, SimilarityType.PRODUCT, "batch", 0, 40, 10, entropy)
        overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(batchFinishedApp, fairFinishedApp)
        print("entropy: " + str(entropy) + ", " + str(batchMakespan) + ", " + str(unfairness))'''
        
        
    '''workload = "memory"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    print("fair: " + str(fairMakespan))
    
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print("perf: " + str(perfMakespan) + ", " + str(unfairness))
    
    workload = "memorycpu"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    print("fair: " + str(fairMakespan))
    
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print("perf: " + str(perfMakespan) + ", " + str(unfairness))
    
    workload = "memorycpudisk"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    print("fair: " + str(fairMakespan))
    
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print("perf: " + str(perfMakespan) + ", " + str(unfairness))
    
    workload = "memorycpudisknetwork"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    print("fair: " + str(fairMakespan))
    
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print("perf: " + str(perfMakespan) + ", " + str(unfairness))'''
    
    workload = "memory"
    fairMakespan, fairFinishedApp = execSimulation(workload, 1)
    print("fair: " + str(fairMakespan))
    
    perfMakespan, perfFinishedApp = execSimulation(workload, 0)
    overallFairness, unfairness, relativeAppFairness = getFairnessStatistic(perfFinishedApp, fairFinishedApp)
    print("perf: " + str(perfMakespan) + ", " + str(unfairness))