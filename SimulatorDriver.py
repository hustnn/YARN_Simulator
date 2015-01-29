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
            

def execSimulation(workloadFile, tradeoffFactor):
    cluster = Cluster(10)
    fileList = [{"name": "wiki-40G", "size": Configuration.BLOCK_SIZE * 4 * 10}]
    for f in fileList:
        hadoopFile = File(f["name"], f["size"])
        cluster.uploadFile(hadoopFile)
        
    scheduler = YARNScheduler(cluster, True, tradeoffFactor)
    
    scheduler.createQueue("queue1", "PACKING", True, "root")
    
    #queueWorkloads = {"queue1": "q1-workload"}
    queueWorkloads = {"queue1": workloadFile}
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, queueWorkloads, cluster)
    
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
        
    for app in scheduler.getFinishedApps():
        print(app.getApplicationID(), app.getJob().getFinishTime())
        
    #for node in scheduler.getAllNodes():
    #    print(str(node.getAvailableResource()))
    
    makespan = simulationStepCount * Configuration.SIMULATION_STEP
    print("simulation end: " + str(makespan))
    
    finishedApp = scheduler.getFinishedAppsInfo()
    
    return makespan, finishedApp

if __name__ == '__main__':
    execSimulation("q1-workload", 0)
    