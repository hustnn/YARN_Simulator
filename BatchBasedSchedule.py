'''
Created on Apr 8, 2015

@author: hustnn
'''

import Configuration

from JobGenerator import JobGenerator
from Utility import Utility

import time
import itertools
from datetime import datetime
from random import randint

def genJob(num):
    fileName = Configuration.WORKLOAD_PATH + "workloadSet"
    f = open(fileName, "r")
    lines = f.readlines()
    f.close()
    jobCount = 1
    jobList = []
    for l in lines:
        for i in range(num):
            items = l.split(",")
            #numOfTask = int(items[0])
            #taskExecTime = int(items[1])
            #submissionTime = int(items[2])
            memory = int(items[3])
            cpu = int(items[4])
            disk = int(items[5])
            network = int(items[6])
            #job = JobGenerator.genComputeIntensitveJob(str(jobCount), numOfTask, memory, cpu, disk, network, taskExecTime, submissionTime)
            jobList.append((memory, cpu, disk, network))
            
    return jobList


def genJobCateList(jobCate, num):
    l = []
    for i in range(len(jobCate)):
        for j in range(num):
            l.append(jobCate[i])
            
    return l
        
def swap(l, i, j):
    tmp = l[i]
    l[i] = l[j]
    l[j] = tmp


RES = [0]


def checkSwapValid(jobList, i, j):
    for k in range(i, j):
        if jobList[k] == jobList[j]:
            return False
    return True


def genCombination(jobList, i, n):
    if (i == n - 1):
        #print(jobList)
        RES[0] = RES[0] + 1
        #RES.append(list(jobList))
        #RES.append(genAverageEntropy(jobList, 4))
        return
    else:
        for k in range(i, n):
            if checkSwapValid(jobList, i, k):
                swap(jobList, k, i)
                genCombination(jobList, i + 1, n)
                swap(jobList, k, i)


def genAverageEntropy(l, batchSize):
    totalEntropy = 0
    c = len(l) / batchSize
    for i in range(c):
        s = l[i * batchSize: (i + 1) * batchSize]
        entropy = Utility.calEntropyOfVectorList(s, 4)
        totalEntropy += entropy
    aveEn = float(totalEntropy) / c
    return aveEn
    #print(aveEn)


def getNextPermu(A , n):
    j = n - 2
    while(A[j] >= A[j + 1] and j >= 0):
        j -= 1
        
    if (j < 0):
        return False
    
    i = n - 1
    while(A[j] >= A[i]):
        i -= 1
        
    swap(A, j, i)
    
    l = j + 1
    r = n - 1
    while(l < r):
        swap(A, l, r)
        l += 1
        r -= 1
        
    return True


def genLexiPermu(A, n):
    num = 0
    sorted(A)
    while(True):
        num += 1
        print(A)
        if not getNextPermu(A, n):
            break
    print(num)
    
    
def swapItemByWindow(A, swapNum, windowNum, windowSize):
    for i in range(swapNum):
        r1 = randint(0, windowNum - 1)
        r2 = randint(0, windowNum - 1)
        w1 = A[r1]
        w2 = A[r2]
        e1 = randint(0, windowSize - 1)
        e2 = randint(0, windowSize - 1)
        tmp = w1[e1]
        w1[e1] = w2[e2]
        w2[e2] = tmp
    return A


def genWindowBasedList(A, windowSize):
    w = []
    for i in range(len(A) / windowSize):
        w.append(A[i * windowSize : (i + 1) * windowSize])
        
    return w
    
    
def genAverageEntropyByWindow(windowList):
    totalEntropy = 0
    for w in windowList:
        entropy = Utility.calEntropyOfVectorList(w, 4)
        totalEntropy += entropy
    aveEn = float(totalEntropy) / len(windowList)
    return aveEn


if __name__ == '__main__':
    #jobList = genJob(4)
    
    #genCombination(jobList, 0, len(jobList))
    #print(len(RES))
    
    #print(str(datetime.now()))
    #l = genJobCateList([1, 1, 2, 2, 3, 3], 1)
    #genLexiPermu(l, len(l))
    #print(str(datetime.now()))
    
    '''genCombination(l, 0, len(l))
    print(RES[0])
    print(str(datetime.now()))'''
    
    
    # Gen combination by swapping
    l = genJobCateList([1,2,3,4], 10)
    w = genWindowBasedList(l, 10)
    swapNumList = [0]
    repeatNum = 10
    for i in range(1, 20):
        for j in range(repeatNum):
            swapNumList.append(i)
            
    for i in swapNumList:
        beforeSwap = list(w)
        afterSwap = swapItemByWindow(beforeSwap, i, 4, 10)
        e = genAverageEntropyByWindow(afterSwap)
        print(e)