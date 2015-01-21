'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Resource import Resource 
from File import File
from Cluster import Cluster
from YARNScheduler import YARNScheduler
from WorkloadGenerator import WorkloadGenerator

import Configuration

import time

if __name__ == '__main__':
    cluster = Cluster(10)
    fileList = [{"name": "wiki-40G", "size": Configuration.BLOCK_SIZE * 4 * 10}]
    for f in fileList:
        file = File(f["name"], f["size"])
        cluster.uploadFile(file)
        
    scheduler = YARNScheduler(cluster, True, 1)
    
    scheduler.createQueue("queue1", "DRF", True, "root")
    
    queueWorkloads = {"queue1": "q1-workload"}
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, queueWorkloads, cluster)
    
    '''for k in queueWorkloads.keys():
        v = workloadGen.getQueues()[k]
        for job in v["jobList"]:
            print(job.getSubmissionTime(), len(job.getTaskList()))'''
        
    
    simulationStepCount = 0
    
    while True:
        if workloadGen.allJobsSubmitted() and len(scheduler.getAllApplications()) == 0:
            break
        
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        workloadGen.submitJobs(currentTime, scheduler)
        scheduler.simulate(Configuration.SIMULATION_STEP, currentTime)
        simulationStepCount += 1
        
        #time.sleep(1)
        print("\n")
        
    print("simulation end: " + str(simulationStepCount))
    