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



if __name__ == '__main__':
    cluster = Cluster(10)
    fileList = [{"name": "wiki-40G", "size": Configuration.BLOCK_SIZE * 4 * 40}]
    for f in fileList:
        file = File(f["name"], f["size"])
        cluster.uploadFile(file)
        
    scheduler = YARNScheduler(cluster)
    
    scheduler.createQueue("queue1", "fair", True, "root")
    
    queueWorkloads = {"queue1": "q1-workload"}
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, queueWorkloads, cluster)
    
    for k in queueWorkloads.keys():
        v = workloadGen.getQueues()[k]
        for job in v["jobList"]:
            print(job.getSubmissionTime(), len(job.getTaskList()))
        
    
    simulationStepCount = 0
    
    while True:
        if workloadGen.allJobsSubmitted() and len(scheduler.getAllApplications()) == 0:
            break
        
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        workloadGen.submitJobs(currentTime, scheduler)
        scheduler.simulate(Configuration.SIMULATION_STEP, currentTime)
        simulationStepCount += 1
    