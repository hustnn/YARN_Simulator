'''
Created on Jan 30, 2015

@author: zhaojie
'''

import Configuration

from Utility import Utility

from numpy import *
from pylab import *
#from matplotlib import rc, rcParams
# Pyplot is a module within the matplotlib library for plotting
#import matplotlib.pyplot as plt
import copy
import sys

MEMORY_LIST = [2048, 4096, 6144, 8192, 10240, 12288, 14336, 16384, 18432, 20480, 22528, 24576]
CPU_LIST = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
DISK_LIST = [20, 40, 60, 80, 100, 128, 148, 168, 188, 208, 228, 256]
NETWORK_LIST = [10, 20, 30, 40, 50, 64, 74, 84, 94, 104, 114, 128]

DEFAULT_L_INDEX = 5
DEFAULT_S_INDEX = 0
TOTAL_INDEX_LENGTH = 12

def getJobStr(jobType, numOfTasks, memory, cpu, disk, network, taskExecTime, arrivalTime):
    jobInfo = [jobType, str(numOfTasks), str(memory), str(cpu), str(disk), str(network), str(taskExecTime), str(arrivalTime)]
    return ",".join(jobInfo)


def genAllSameOrMixedCombination(numOfJobs, numOfTasks, taskExecTime):
    jobs = {"allL-allS": [],
            "allL-mixed": [],
            "allS-mixed": [],
            "mixed-mixed": []}
    
    allL = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    allS = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    
    memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    
    half = numOfJobs / 2
    for i in range(half):
        jobs["allL-allS"].append(allL)
        jobs["allL-mixed"].append(allL)
        jobs["allS-mixed"].append(allS)
        
    for i in range(half):
        jobs["allL-allS"].append(allS)
        jobs["allL-mixed"].append(memoryDominant)
        jobs["allS-mixed"].append(memoryDominant)
        
    quarter = numOfJobs / 4
    mixed = [memoryDominant, cpuDominant, diskDominant, networkDominant]
    for job in mixed:
        for i in range(quarter):
            jobs["mixed-mixed"].append(job)
            
    for k, v in jobs.items():
        filename = Configuration.WORKLOAD_PATH + k
        f = open(filename, "w")
        for job in v:
            f.write(job + "\n")
        f.close()
        
        
def genJobListByRatio(filename, numOfJobs, jobList, firstJobRatio):
    pass
        
        
def genAllLAndAllSVaryingL(numOfJobs, numOfTasks, taskExecTime):
    for largeIndex in range(TOTAL_INDEX_LENGTH):
        allL = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allS = getJobStr("compute", numOfTasks, MEMORY_LIST[0], CPU_LIST[0], DISK_LIST[0], NETWORK_LIST[0], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allL-allS" + "-L" + str(largeIndex) + "-S" + str(0)
        jobs = [allL, allS]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
                
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        

def genAllLAndAllSVaryingS(numOfJobs, numOfTasks, taskExecTime):
    for smallIndex in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 4
        allL = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allS = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allL-allS" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [allL, allS]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
                
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genAllLAndAllSVaryingBoth(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 11 - index
        smallIndex = 0 + index
        allL = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allS = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allL-allS" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [allL, allS]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
                
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genAllLAndMixedVaryingL(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH):
        largeIndex = index
        smallIndex = 0
        allL = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allL-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [allL, memoryDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genAllLAndMixedVaryingS(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 5
        smallIndex = index
        allL = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allL-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [allL, memoryDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genAllLAndMixedVaryingBoth(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 11 - index
        smallIndex = 0 + index
        allL = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allL-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [allL, memoryDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genAllSAndMixedVaryingL(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH):
        largeIndex = index
        smallIndex = 0
        allS = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allS-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [allS, memoryDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genAllSAndMixedVaryingS(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 5
        smallIndex = index
        allS = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allS-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [allS, memoryDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genAllSAndMixedVaryingBoth(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 11 - index
        smallIndex = 0 + index
        allS = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "allS-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [allS, memoryDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 2):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genMixedAndMixedVaryingL(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH):
        largeIndex = index
        smallIndex = 0
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "mixed-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 4):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
        
def genMixedAndMixedVaryingS(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 5
        smallIndex = index
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "mixed-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 4):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
            
            
def genMixedAndMixedVaryingBoth(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 11 - index
        smallIndex = 0 + index
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        filename = Configuration.WORKLOAD_PATH + "mixed-mixed" + "-L" + str(largeIndex) + "-S" + str(smallIndex)
        jobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        jobList = []
        for job in jobs:
            for i in range(numOfJobs / 4):
                jobList.append(job)
        
        f = open(filename, "w")
        for job in jobList:
            f.write(job + "\n")
        f.close()
        
            
def genResourceTypeCombination(numOfJobs, numOfTasks, taskExecTime):
    jobs = {"oneType": [],
            "twoType": [],
            "threeType": [],
            "fourType": []}
    
    memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)

    allSmall = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)

    oneJobs = [memoryDominant, allSmall]
    for job in oneJobs:
        for i in range(numOfJobs / 2):
            jobs["oneType"].append(job)
        
    twoJobs = [memoryDominant, cpuDominant]
    for job in twoJobs:
        for i in range(numOfJobs / 2):
            jobs["twoType"].append(job)
            
    threeJobs = [memoryDominant, cpuDominant, diskDominant]
    for job in threeJobs:
        for i in range(numOfJobs / 3):
            jobs["threeType"].append(job)
            
    fourJobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
    for job in fourJobs:
        for i in range(numOfJobs / 4):
            jobs["fourType"].append(job)
            
    for k, v in jobs.items():
        filename = Configuration.WORKLOAD_PATH + k
        f = open(filename, "w")
        for job in v:
            f.write(job + "\n")
        f.close()
        
        
def genResourceTypeCombinationVaryingL(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH):
        largeIndex = index
        smallIndex = 0
        
        jobs = {"oneType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "twoType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "threeType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "fourType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): []}
        
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allSmall = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)

        oneJobs = [memoryDominant, allSmall]
        for job in oneJobs:
            for i in range(numOfJobs / 2):
                jobs["oneType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
            
        twoJobs = [memoryDominant, cpuDominant]
        for job in twoJobs:
            for i in range(numOfJobs / 2):
                jobs["twoType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        threeJobs = [memoryDominant, cpuDominant, diskDominant]
        for job in threeJobs:
            for i in range(numOfJobs / 3):
                jobs["threeType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        fourJobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        for job in fourJobs:
            for i in range(numOfJobs / 4):
                jobs["fourType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        for k, v in jobs.items():
            filename = Configuration.WORKLOAD_PATH + k
            f = open(filename, "w")
            for job in v:
                f.write(job + "\n")
            f.close()
            
            
def genResourceTypeCombinationVaryingS(numOfJobs, numOfTasks, taskExecTime):
    largeIndex = 3
    #for index in range(TOTAL_INDEX_LENGTH / 2):
    for index in range(largeIndex + 1):
        smallIndex = index
        
        jobs = {"oneType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "twoType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "threeType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "fourType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): []}
        
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allSmall = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)

        oneJobs = [memoryDominant, allSmall]
        for job in oneJobs:
            for i in range(numOfJobs / 2):
                jobs["oneType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
            
        twoJobs = [memoryDominant, cpuDominant]
        for job in twoJobs:
            for i in range(numOfJobs / 2):
                jobs["twoType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        threeJobs = [memoryDominant, cpuDominant, diskDominant]
        for job in threeJobs:
            for i in range(numOfJobs / 3):
                jobs["threeType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        fourJobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        for job in fourJobs:
            for i in range(numOfJobs / 4):
                jobs["fourType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        for k, v in jobs.items():
            filename = Configuration.WORKLOAD_PATH + k
            f = open(filename, "w")
            for job in v:
                f.write(job + "\n")
            f.close()
            
            
def genResourceTypeCombinationVaryingBoth(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH):
        largeIndex = 11 - index
        smallIndex = 0 + index
        
        jobs = {"oneType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "twoType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "threeType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "fourType" + "-L" + str(largeIndex) + "-S" + str(smallIndex): []}
        
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allSmall = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)

        oneJobs = [memoryDominant, allSmall]
        for job in oneJobs:
            for i in range(numOfJobs / 2):
                jobs["oneType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
            
        twoJobs = [memoryDominant, cpuDominant]
        for job in twoJobs:
            for i in range(numOfJobs / 2):
                jobs["twoType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        threeJobs = [memoryDominant, cpuDominant, diskDominant]
        for job in threeJobs:
            for i in range(numOfJobs / 3):
                jobs["threeType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        fourJobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        for job in fourJobs:
            for i in range(numOfJobs / 4):
                jobs["fourType" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        for k, v in jobs.items():
            filename = Configuration.WORKLOAD_PATH + k
            f = open(filename, "w")
            for job in v:
                f.write(job + "\n")
            f.close()
        
        
def genDominantResourceCombination(numOfJobs, numOfTasks, taskExecTime):
    jobs = {"oneDominant": [],
            "twoDominant": [],
            "threeDominant": [],
            "fourDominant": []}
    
    memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    
    memoryCPUDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    memoryDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    memoryNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    CPUDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    CPUNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    DiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    
    memoryCPUDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    memoryCPUNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    memoryDiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    cpuDiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    
    allLarge = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_L_INDEX], CPU_LIST[DEFAULT_L_INDEX], DISK_LIST[DEFAULT_L_INDEX], NETWORK_LIST[DEFAULT_L_INDEX], taskExecTime, 0)
    allSmall = getJobStr("compute", numOfTasks, MEMORY_LIST[DEFAULT_S_INDEX], CPU_LIST[DEFAULT_S_INDEX], DISK_LIST[DEFAULT_S_INDEX], NETWORK_LIST[DEFAULT_S_INDEX], taskExecTime, 0)
    
    oneJobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
    for job in oneJobs:
        for i in range(numOfJobs / 4):
            jobs["oneDominant"].append(job)
            
    twoJobs = [memoryCPUDominant, memoryDiskDominant, memoryNetworkDominant, CPUDiskDominant, CPUNetworkDominant, DiskNetworkDominant]
    for job in twoJobs:
        for i in range(numOfJobs / 6):
            jobs["twoDominant"].append(job)
            
    threeJobs = [memoryCPUDiskDominant, memoryCPUNetworkDominant, memoryDiskNetworkDominant, cpuDiskNetworkDominant]
    for job in threeJobs:
        for i in range(numOfJobs / 4):
            jobs["threeDominant"].append(job)
            
    fourJobs = [allLarge, allSmall]
    for job in fourJobs:
        for i in range(numOfJobs / 2):
            jobs["fourDominant"].append(job)
            
    for k, v in jobs.items():
        filename = Configuration.WORKLOAD_PATH + k
        f = open(filename, "w")
        for job in v:
            f.write(job + "\n")
        f.close()
        
        
def genDominantResourceCombinationVaryingL(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH):
        largeIndex = index
        smallIndex = 0 
    
        jobs = {"oneDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "twoDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "threeDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "fourDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): []}
        
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        memoryCPUDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        CPUDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        CPUNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        DiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        memoryCPUDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryCPUNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        memoryDiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        cpuDiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        allLarge = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allSmall = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        
        oneJobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        for job in oneJobs:
            for i in range(numOfJobs / 4):
                jobs["oneDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        twoJobs = [memoryCPUDominant, memoryDiskDominant, memoryNetworkDominant, CPUDiskDominant, CPUNetworkDominant, DiskNetworkDominant]
        for job in twoJobs:
            for i in range(numOfJobs / 6):
                jobs["twoDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        threeJobs = [memoryCPUDiskDominant, memoryCPUNetworkDominant, memoryDiskNetworkDominant, cpuDiskNetworkDominant]
        for job in threeJobs:
            for i in range(numOfJobs / 4):
                jobs["threeDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        fourJobs = [allLarge, allSmall]
        for job in fourJobs:
            for i in range(numOfJobs / 2):
                jobs["fourDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        for k, v in jobs.items():
            filename = Configuration.WORKLOAD_PATH + k
            f = open(filename, "w")
            for job in v:
                f.write(job + "\n")
            f.close()
            
            
def genDominantResourceCombinationVaryingS(numOfJobs, numOfTasks, taskExecTime):
    largeIndex = 6
    #for index in range(TOTAL_INDEX_LENGTH / 2):
    for index in range(largeIndex + 1):
        smallIndex = index
    
        jobs = {"oneDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "twoDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "threeDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "fourDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): []}
        
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        memoryCPUDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        CPUDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        CPUNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        DiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        memoryCPUDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryCPUNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        memoryDiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        cpuDiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        allLarge = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allSmall = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        
        oneJobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        for job in oneJobs:
            for i in range(numOfJobs / 4):
                jobs["oneDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        twoJobs = [memoryCPUDominant, memoryDiskDominant, memoryNetworkDominant, CPUDiskDominant, CPUNetworkDominant, DiskNetworkDominant]
        for job in twoJobs:
            for i in range(numOfJobs / 6):
                jobs["twoDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        threeJobs = [memoryCPUDiskDominant, memoryCPUNetworkDominant, memoryDiskNetworkDominant, cpuDiskNetworkDominant]
        for job in threeJobs:
            for i in range(numOfJobs / 4):
                jobs["threeDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        fourJobs = [allLarge, allSmall]
        for job in fourJobs:
            for i in range(numOfJobs / 2):
                jobs["fourDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        for k, v in jobs.items():
            filename = Configuration.WORKLOAD_PATH + k
            f = open(filename, "w")
            for job in v:
                f.write(job + "\n")
            f.close()
            
            
def genDominantResourceCombinationVaryingBoth(numOfJobs, numOfTasks, taskExecTime):
    for index in range(TOTAL_INDEX_LENGTH / 2):
        largeIndex = 11 - index
        smallIndex = 0 + index
    
        jobs = {"oneDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "twoDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "threeDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): [],
                "fourDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex): []}
        
        memoryDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        cpuDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        diskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        networkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        memoryCPUDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        CPUDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        CPUNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        DiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        memoryCPUDiskDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        memoryCPUNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[smallIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        memoryDiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[smallIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        cpuDiskNetworkDominant = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        
        allLarge = getJobStr("compute", numOfTasks, MEMORY_LIST[largeIndex], CPU_LIST[largeIndex], DISK_LIST[largeIndex], NETWORK_LIST[largeIndex], taskExecTime, 0)
        allSmall = getJobStr("compute", numOfTasks, MEMORY_LIST[smallIndex], CPU_LIST[smallIndex], DISK_LIST[smallIndex], NETWORK_LIST[smallIndex], taskExecTime, 0)
        
        oneJobs = [memoryDominant, cpuDominant, diskDominant, networkDominant]
        for job in oneJobs:
            for i in range(numOfJobs / 4):
                jobs["oneDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        twoJobs = [memoryCPUDominant, memoryDiskDominant, memoryNetworkDominant, CPUDiskDominant, CPUNetworkDominant, DiskNetworkDominant]
        for job in twoJobs:
            for i in range(numOfJobs / 6):
                jobs["twoDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        threeJobs = [memoryCPUDiskDominant, memoryCPUNetworkDominant, memoryDiskNetworkDominant, cpuDiskNetworkDominant]
        for job in threeJobs:
            for i in range(numOfJobs / 4):
                jobs["threeDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        fourJobs = [allLarge, allSmall]
        for job in fourJobs:
            for i in range(numOfJobs / 2):
                jobs["fourDominant" + "-L" + str(largeIndex) + "-S" + str(smallIndex)].append(job)
                
        for k, v in jobs.items():
            filename = Configuration.WORKLOAD_PATH + k
            f = open(filename, "w")
            for job in v:
                f.write(job + "\n")
            f.close()
            
            
def genJob(jobType, numOfTasks, memory, cpu, disk, network, taskExecTime):
    jobInfo = [jobType, str(numOfTasks), str(memory), str(cpu), str(disk), str(network), str(taskExecTime)]
    return ",".join(jobInfo)

def addTime(job, t):
    return job + "," + str(t)


def genMicroWorkload(filename):
    nonDominant = genJob("compute", 10, 2048, 1, 20, 10, 5)
    memoryDominant = genJob("compute", 10, 12288, 1, 20, 10, 5)
    cpuDominant = genJob("compute", 10, 2048, 6, 20, 10, 5)
    diskDominant = genJob("compute", 10, 2048, 1, 128, 10, 5)
    networkDominant = genJob("compute", 10, 2048, 1, 20, 64, 5)
    
    jobList = []
    
    '''for i in range(40):
        job = addTime(nonDominant, 0)
        jobList.append(job)
        
    for i in range(20):
        job = addTime(memoryDominant, 0)
        jobList.append(job)'''
        
    for i in range(40):
        job = addTime(nonDominant, 0)
        jobList.append(job)
        
    for i in range(20):
        job = addTime(cpuDominant, 0)
        jobList.append(job)
        
    '''for i in range(40):
        job = addTime(nonDominant, 0)
        jobList.append(job)
        
    for i in range(20):
        job = addTime(memoryDominant, 0)
        jobList.append(job)
        
    for i in range(40):
        job = addTime(nonDominant, 0)
        jobList.append(job)
        
    for i in range(20):
        job = addTime(memoryDominant, 0)
        jobList.append(job)'''
    
    '''for i in range(10):
        job = addTime(memoryDominant, 0)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(cpuDominant, 0)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(diskDominant, 0)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(networkDominant, 0)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(memoryDominant, 0)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(cpuDominant, 0)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(diskDominant, 0)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(networkDominant, 0)
        jobList.append(job)'''
        
        
    '''for i in range(10):
        job = addTime(memoryDominant, 60)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(cpuDominant, 60)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(diskDominant, 60)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(networkDominant, 60)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(memoryDominant, 60)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(cpuDominant, 60)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(diskDominant, 60)
        jobList.append(job)
        
    for i in range(10):
        job = addTime(networkDominant, 60)
        jobList.append(job)'''
        
    
        
    f = open(Configuration.WORKLOAD_PATH + filename, "w")
    for job in jobList:
        f.write(job + "\n")
    f.close()
    
    
def genDiffEntropy():
    count = 24
    memoryIntensive = "compute,10,12288,1,20,10,5,0"
    memoryVector = [12288, 1, 20, 10]
    cpuIntensive = "compute,10,2048,6,20,10,5,0"
    cpuVector = [2048, 6,  20, 10]
    diskIntensive = "compute,10,2048,1,128,10,5,0"
    diskVector = [2048, 1, 128, 10]
    netIntensive = "compute,10,2048,1,20,64,5,0"
    netVector = [2048, 1, 20, 64]
    
    a1 = [memoryIntensive] * 24
    v1 = [memoryVector] * 24
    
    a2 = [cpuIntensive] * 24
    v2 = [memoryVector] * 18 + [cpuVector] * 6
    
    a3 = [diskIntensive] * 24
    v3 = [memoryVector] * 8 + [cpuVector] * 8 + [diskVector] * 8
    
    a4 = [netIntensive] * 24
    v4 = [memoryVector] * 8 + [cpuVector] * 6 + [diskVector] * 6 + [netVector] * 2
    
    print(Utility.calEntropyOfVectorList(v1))
    print(Utility.calEntropyOfVectorList(v2))
    print(Utility.calEntropyOfVectorList(v3))
    print(Utility.calEntropyOfVectorList(v4))
    
    
def computeOverhead():
    memoryVector = [12288, 1, 20, 10]
    cpuVector = [2048, 6,  20, 10]
    diskVector = [2048, 1, 128, 10]
    netVector = [2048, 1, 20, 64]
    count = 25
    
    import datetime
    
    for i in [1, 10, 100]:
        c = count * i
        v = [memoryVector] * c + [cpuVector] * c + [diskVector] * c + [netVector] * c
        print(datetime.datetime.now().time())
        print(Utility.calEntropyOfVectorList(v))
        print(datetime.datetime.now().time())
        
        
def fitting():
    # 
    x1 = [1, 1.05, 1.28, 1.4284, 1.58, 1.8676, 2] 
    y1 = [0, 0.09, 0.12, 0.17, 0.25, 0.42, 0.5]
    y2 = [0.1, 0.1, 0.04, 0.015, 0, 0, 0]
    
    coefficients1 = polyfit(x1,y1,3)
    print coefficients1
    

if __name__ == '__main__':
    
    #genMicroWorkload("micro-workload")
    
    #genDiffEntropy()
    
    #computeOverhead()
    
    fitting()
    
    #basic
    '''genAllSameOrMixedCombination(24, 10, 5)
    genResourceTypeCombination(24, 10, 5)
    genDominantResourceCombination(24, 10, 5)
    
    #all large and all small
    genAllLAndAllSVaryingL(24, 10, 5)
    genAllLAndAllSVaryingS(24, 10, 5)
    genAllLAndAllSVaryingBoth(24, 10, 5)
    
    #all large and mixed
    genAllLAndMixedVaryingL(24, 10, 5)
    genAllLAndMixedVaryingS(24, 10, 5)
    genAllLAndMixedVaryingBoth(24, 10, 5)
    
    #all small and mixed
    genAllSAndMixedVaryingL(24, 10, 5)
    genAllSAndMixedVaryingS(24, 10, 5)
    genAllSAndMixedVaryingBoth(24, 10, 5)
    
    #mixed and mixed
    genMixedAndMixedVaryingL(24, 10, 5)
    genMixedAndMixedVaryingS(24, 10, 5)
    genMixedAndMixedVaryingBoth(24, 10, 5)
    
    #resource type combination 
    genResourceTypeCombinationVaryingL(24, 10, 5)
    genResourceTypeCombinationVaryingS(24, 10, 5)
    genResourceTypeCombinationVaryingBoth(24, 10, 5)
    
    #dominant resource combination
    genDominantResourceCombinationVaryingL(24, 10, 5)
    genDominantResourceCombinationVaryingS(24, 10, 5)
    genDominantResourceCombinationVaryingBoth(24, 10, 5)'''
    