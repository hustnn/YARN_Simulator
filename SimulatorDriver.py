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

if __name__ == '__main__':
    cluster = Cluster(10)
    fileList = [{"name": "wiki-40G", "size": Configuration.BLOCK_SIZE * 4 * 10}]
    for f in fileList:
        hadoopFile = File(f["name"], f["size"])
        cluster.uploadFile(hadoopFile)
        
    scheduler = YARNScheduler(cluster, True, 1)
    
    scheduler.createQueue("queue1", "PACKING", True, "root")
    
    queueWorkloads = {"queue1": "q1-workload"}
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
        print("currentTime: " + str(currentTime))
        workloadGen.submitJobs(currentTime, scheduler)
        scheduler.simulate(Configuration.SIMULATION_STEP, currentTime)
        simulationStepCount += 1
        
        #time.sleep(1)
        #print("\n")
        
    for app in scheduler.getFinishedApps():
        print(app.getApplicationID(), app.getJob().getFinishTime())
        
    #for node in scheduler.getAllNodes():
    #    print(str(node.getAvailableResource()))
        
    print("simulation end: " + str(simulationStepCount))
    