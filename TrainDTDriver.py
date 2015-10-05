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


def swapJobTypeListInterWindow(jobTypeWindows, swapNumList):
    windowList = []
    for swapNum in swapNumList:
        wAfterSwap = swapWindow(jobTypeWindows, swapNum)
        sortWindowBasedList(wAfterSwap)
        for window in wAfterSwap:
            windowList.append(window)
            
    return windowList


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


def genRandomJobTypeList(num, jobTypes):
    jobTypeList = []
    numJobType = len(jobTypes)
    for i in range(num):
        jobTypeList.append(jobTypes[randint(0, numJobType - 1)])
        
    return jobTypeList
    

def genWinBasedJobTypeList(jobTypeList, windowSize, swapNumList):
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


def genYARNJobs(jobTypeList, jobInfoList):
    jobs = []
    jobCount = 0
    for jobType in jobTypeList:
        jobInfo = jobInfoList[jobType]
        jobCount += 1
        job = JobGenerator.genComputeIntensitveJob(str(jobCount), jobInfo["NumOfTasks"], jobInfo["Memory"], jobInfo["CPU"], jobInfo["Disk"], jobInfo["Network"], 
                                                   jobInfo["TaskExecTime"], jobInfo["SubmitTime"])
        jobs.append(job)
    return jobs


def calYARNJobTwoDComplementarity(jobs, nodeMemory, nodeCPU):
    sumOfCom = 0
    
    # vector complementarity
    for jobA in jobs:
        for jobB in jobs:
            if jobA.getJobID() != jobB.getJobID():
                jobARes = jobA.getResource()
                jobBRes = jobB.getResource()
                jobA2DResVec = [float(jobARes.getMemory()) / nodeMemory, float(jobARes.getCPU()) / nodeCPU]
                jobB2DResVec = [float(jobBRes.getMemory()) / nodeMemory, float(jobBRes.getCPU()) / nodeCPU]
                sumOfCom += Utility.calComplementarityOfTwoDVectors(jobA2DResVec, jobB2DResVec)
    
    #avoid repeat computation
    comple = sumOfCom / (2.0 * len(jobs))
     
    # vector Symmetry
    jobVectorList = []
    for job in jobs:
        jobVectorList.append([job.getResource().getMemory(), job.getResource().getCPU()])
    symmetricity = Utility.calSymmetryOfTwoDVectors(jobVectorList)
    
    return comple * symmetricity * symmetricity, comple, symmetricity


def loadJobInfo(fileName):
    jobInfoList = []
    f = open(fileName, "r")
    lines = f.readlines()
    for line in lines:
        items = line.split(",")
        jobInfoList.append({"NumOfTasks": int(items[0]),
                                 "TaskExecTime": int(items[1]),
                                 "SubmitTime": int(items[2]),
                                 "Memory": int(items[3]),
                                 "CPU": int(items[4]),
                                 "Disk": int(items[5]),
                                 "Network": int(items[6])})
    f.close()
    return jobInfoList


def compareMakespanAndFairness(finishedApps1, makespan1, finishedApps2, makespan2):
    count = 0
    reduction = 0.0
    for k in finishedApps1.keys():
        count += 1
        finishTimeOfCurApp = finishedApps1[k]
        finishTimeOfBaseApp = finishedApps2[k]
        if finishTimeOfCurApp > finishTimeOfBaseApp:
            reduction += float(finishTimeOfCurApp - finishTimeOfBaseApp) / finishTimeOfBaseApp
            
    # the bigger the perf value is, the better performance the scheduler is 
    # the smaller the fairness value is , more fair the scheduler is,  
    return {"perf": 1 - float(makespan1) / makespan2, "fairness": float(reduction) / count}
    

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
    
    
def calculateSlowdown(appFinishTime, bestAppFinishTime):
    if appFinishTime <= bestAppFinishTime:
        return 0.0
    else:
        return float(appFinishTime - bestAppFinishTime) / bestAppFinishTime


def schedule(clusterSize, queueName, jobList, policy, considerIO):
    cluster = Cluster(clusterSize)
    
    scheduler = YARNScheduler(cluster, considerIO)
    #scheduler.createQueue("queue1", policy, True, "root")
    scheduler.createQueue(queueName, policy, True, "root")
    
    workloadGen = WorkloadGenerator(Configuration.SIMULATION_PATH, Configuration.WORKLOAD_PATH, {queueName: ""}, cluster)
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


def execSimulation(clusterSize, queueName, jobList, policy):
    makespan, finishedApp = schedule(clusterSize, queueName, jobList, policy)
    return finishedApp, makespan

        

if __name__ == '__main__':
    jobInfoList = loadJobInfo(Configuration.WORKLOAD_PATH + "YARNJobInfo")
    
    v1 = [0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5]
    v2 = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5]
    '''vec = [v1, v2]
    for v in vec:
        jobs = genYARNJobs(v, jobInfoList)
        comple = calYARNJobTwoDComplementarity(jobs, 20, 20)'''
    
    '''vec = [0, 0]
    jobs = genYARNJobs(vec, jobInfoList)
    comple = calYARNJobTwoDComplementarity(jobs, 20, 20)
    print(vec, comple)
    
    print("***")'''
    
    numJobsPerGroup = 24
    clusterSize = 5
    numOfGroup = 10
    yarnJobTypes = [0, 1, 2, 3, 4, 5]
    swapNumList = [20, 40, 60, 80, 100, 200]
    
    jobTypeList = genRandomJobTypeList(numJobsPerGroup * numOfGroup, yarnJobTypes)
    jobTypeList.sort()
    windowBasedList = genWindowBasedList(jobTypeList, numJobsPerGroup)
    swappedWindowList = swapJobTypeListInterWindow(windowBasedList, swapNumList)
    
    jobsToSchedule = []
    for window in swappedWindowList:
        jobs = genYARNJobs(window, jobInfoList)
        finalComple, comple, symm = calYARNJobTwoDComplementarity(jobs, 20, 20)
        print(window, finalComple, comple, symm)
        jobsToSchedule.append({"Jobs": jobs, "FinalComple": finalComple, "Comple": comple, "Symm": symm})
        
    print("start scheduling")
    print "FinalComple Comple Symm FIFOSlowdown FIFOUnfairness FairSlowdown FairUnfairness DRFSlowdown DRFUnfairness PerfSlowdown PerfUnfairness"
    for jobInfo in jobsToSchedule:
        jobs = jobInfo["Jobs"]
        resVector = []
        for job in jobs:
            resVector.append(job.getResourceVector())
        makespanFIFO, finishedAppFIFO = schedule(clusterSize, "queue1", jobInfo["Jobs"], "FIFO", False)
        makespanFair, finishedAppFair = schedule(clusterSize, "queue1", jobInfo["Jobs"], "fair", False)
        makespanDRF, finishedAppDRF = schedule(clusterSize, "queue1", jobInfo["Jobs"], "MULTIFAIR", False)
        makespanPerf, finishedAppPerf = schedule(clusterSize, "queue1", jobInfo["Jobs"], "MRF", False)
        
        FIFOSlowdown = calculateSlowdown(makespanFIFO, makespanPerf)
        FIFOUnfairness = calculateUnFairness(finishedAppFIFO, finishedAppDRF)
        FairSlowdown = calculateSlowdown(makespanFair, makespanPerf)
        FairUnfairness = calculateUnFairness(finishedAppFair, finishedAppDRF)
        DRFSlowdown = calculateSlowdown(makespanDRF, makespanPerf)
        DRFUnfairness = 0.0
        PerfSlowdown = 0.0
        PerfUnfairness = calculateUnFairness(finishedAppPerf, finishedAppDRF)
        print jobInfo["FinalComple"], jobInfo["Comple"], jobInfo["Symm"], \
              FIFOSlowdown, FIFOUnfairness, FairSlowdown, FairUnfairness, DRFSlowdown, DRFUnfairness, PerfSlowdown, PerfUnfairness
    