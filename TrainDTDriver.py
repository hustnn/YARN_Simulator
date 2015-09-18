'''
Created on Sep 18, 2015

@author: hustnn
'''

import copy

import Configuration
from Cluster import Cluster
from YARNScheduler import YARNScheduler
from WorkloadGenerator import WorkloadGenerator


# generate training dataset used by decision tree, (entropy of workload, fairness SLA, performance)
# the best performance corresponds to the target scheduler
def calTrainingData(workloadScale = 10, windowSize = 10):
    pass


# cal fairness by using the average reduction of job completion time
def calculateUnFairness(finishedApps, fairApps):
    count = 0
    reduction = 0.0
    
    for k in finishedApps.keys():
        count += 1
        curExecTime = finishedApps[k]
        fairExecTime = fairApps[k]
        if curExecTime > fairExecTime:
            red = float(curExecTime - fairExecTime) / fairExecTime
            reduction += red
            
        return float(reduction) / count


def schedule(clusterSize, queueName, jobList, policy):
    cluster = Cluster(clusterSize)
    
    scheduler = YARNScheduler(cluster, True)
    scheduler.createQueue("queue1", policy, True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, {queueName: jobList}, cluster)
    workloadGen.genWorkloadByList(queueName, copy.deepcopy(jobList))
    
    simulationStepCount = 0
    while True:
        if workloadGen.allJobsSubmitted() and len(scheduler.getAllApplications()) == 0:
            break
        
        currentTime = simulationStepCount * Configuration.SIMULATION_STEP
        workloadGen.submitJobs(currentTime, scheduler)
        scheduler.activateWaitingJobs(currentTime)
        scheduler.oldSimulate(Configuration.SIMULATION_STEP, currentTime)
        simulationStepCount += 1
        
    makespan = simulationStepCount * Configuration.SIMULATION_STEP
    finishedApp = scheduler.getFinishedAppsInfo()
    return makespan, finishedApp
        

if __name__ == '__main__':
    pass