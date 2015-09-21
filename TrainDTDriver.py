'''
Created on Sep 18, 2015

@author: hustnn
'''

import copy
from random import randint

import Configuration
from Cluster import Cluster
from YARNScheduler import YARNScheduler
from WorkloadGenerator import WorkloadGenerator
from JobGenerator import JobGenerator
from Utility import Utility


# generate training dataset used by decision tree, (entropy of workload, fairness SLA, performance)
# the best performance corresponds to the target scheduler
def calTrainingData(workloadScale = 10, windowSize = 10):
    pass


def genWindowBasedList(jobTypeList, windowSize):
    windowNum = len(jobTypeList) / windowSize
    w = []
    for i in range(windowNum):
        begin = i * windowSize
        end = min(begin + windowSize, len(jobTypeList))
        w.append(jobTypeList[begin:end])
    
    return w


def swapItemByWindow(A, windowNum, swapNum):
    if windowNum == 1:
        return A
    
    for i in range(swapNum):
        r1 = randint(0, windowNum - 1)
        r2 = r1
        while (r2 == r1):
            r2 = randint(0, windowNum - 1)
        w1 = A[r1]
        w2 = A[r2]
        
        e1 = randint(0, len(w1) - 1)
        e2 = randint(0, len(w2) - 1)
        tmp = w1[e1]
        w1[e1] = w2[e2]
        w2[e2] = tmp
        
    return A


def swapWindow(w, swapNum):
    return swapItemByWindow(copy.deepcopy(w), len(w), swapNum)


def sortWindowBasedList(windowList):
    for i in range(len(windowList)):
        windowList[i] = sorted(windowList[i])
        

def genJobTypeListWithDifferentEntropy(jobTypeList, windowSize, swapNumList):
    res = []
    w = genWindowBasedList(jobTypeList, windowSize)
    for swapNum in swapNumList:
        wAfterSwap = swapWindow(w, swapNum)
        sortWindowBasedList(wAfterSwap)
        for jobs in wAfterSwap:
            res.append(jobs)
            
    return res


def genJobTypeList(num):
    jobTypeList = []
    memoryTypes = [0, 1, 2]
    cpuTypes = [3, 4, 5]
    diskTypes = [6, 7, 8]
    networkTypes = [9, 10, 11]
    jobTypes = [memoryTypes, cpuTypes, diskTypes, networkTypes]
    for resTypes in jobTypes:
        for i in range(num):
            jobTypeList.append(resTypes[randint(0, 2)])
    
    return jobTypeList


def genWinBasedJobTypeList(num, windowSize, swapNumList):
    jobTypeList = genJobTypeList(num)
    return genJobTypeListWithDifferentEntropy(jobTypeList, windowSize, swapNumList)


def genJobInfoList(workloadSet):
    fileName = Configuration.WORKLOAD_PATH + workloadSet
    jobs = []
    f = open(fileName, "r")
    lines = f.readlines()
    f.close()
    for l in lines:
        items = l.split(",")
        jobInfo = {}
        jobInfo["NumOfTasks"] = int(items[0])
        jobInfo["TaskExecTime"] = int(items[1])
        jobInfo["SubmitTime"] = int(items[2])
        jobInfo["Memory"] = int(items[3])
        jobInfo["CPU"] = int(items[4])
        jobInfo["Disk"] = int(items[5])
        jobInfo["Network"] = int(items[6])
        jobInfo["Code"] = int(items[7])
        jobs.append(jobInfo)
    
    return jobs


#job type is the index of jobInfoList
def genJobs(jobTypeList, jobInfoList):
    jobs = []
    codes = []
    jobCount = 0
    for jobType in jobTypeList:
        jobInfo = jobTypeList[jobType]
        jobCount += 1
        numOfTask = jobInfo["NumOfTasks"]
        taskExecTime = jobInfo["TaskExecTime"]
        submissionTime = jobInfo["SubmitTime"]
        memory = jobInfo["Memory"]
        cpu = jobInfo["CPU"]
        disk = jobInfo["Disk"]
        network = jobInfo["Network"]
        code = jobInfo["Code"]
        job = JobGenerator.genComputeIntensitveJob(str(jobCount), numOfTask, memory, cpu, disk, network, 
                                                   taskExecTime, submissionTime)
        jobs.append(job)
        codes.append(code)
    entropy = Utility.calEntropyOfVectorList(codes)
    return jobs, entropy


def execSimulation(clusterSize, queueName, jobList, policy, baselineFinishedApp = None, baselineMakespan = None):
    makespan, finishedApp = schedule(clusterSize, queueName, jobList, policy)
    if baselineFinishedApp == None:
        return finishedApp, makespan
    
    count = 0
    reduction = 0.0
    for k in finishedApp.keys():
        count + 1
        finishTimeOfCurApp = finishedApp[k]
        finishTimeOfBaseApp = baselineFinishedApp[k]
        if finishTimeOfCurApp > finishTimeOfBaseApp:
            reduction += float(finishTimeOfCurApp - finishTimeOfBaseApp) / finishTimeOfBaseApp
    
    # the smaller the fairness value is , more fair the scheduler is, 
    # the bigger the perf value is, the better performance the scheduler is   
    return {"fairness": float(reduction) / count, "perf": 1 - float(makespan) / baselineMakespan}


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
    #scheduler.createQueue("queue1", policy, True, "root")
    scheduler.createQueue(queueName, policy, True, "root")
    
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
    workloadList = genWinBasedJobTypeList(3, 3, [1])
    clusterSize = 10
    queueName = "queue1"
    
    for workload in workloadList:
        jobs, entropy = genJobs(workload, {})
        #execSimulation(clusterSize, queueName, jobList, policy, baselineFinishedApp = None, baselineMakespan = None)
        policy = "DRF"
        finishedAppUnderDRF, makespanUnderDRF = execSimulation(clusterSize, queueName, jobs, policy)
        policyList = ["FIFO", "PERf", "FAIR"]