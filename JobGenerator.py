'''
Created on Jan 10, 2015

@author: niuzhaojie
'''

from Job import Job
from ComputeTask import ComputeTask
from Priority import Priority
from IOTask import IOTask
from FileBlock import FileBlock
from BlockType import BlockType
from Resource import Resource

class JobGenerator(object):
    '''
    classdocs
    '''
    @staticmethod
    def genComputeIntensitveJob(jobID, numOfTask, memory, cpu, disk, network, execTime, submissionTime):
        job = Job(jobID, submissionTime)
        for i in range(numOfTask):
            resource = Resource(memory, cpu, disk, network)
            job.addTask(ComputeTask(jobID + "-computeTask-" + str(i), Priority.NORMAL, resource, execTime))
            
        return job
    
    
    @staticmethod
    def genMapOnlyJob(jobID, inputFile, memory, cpu, disk, network, submissionTime):
        job = Job(jobID, submissionTime)
        blockIndex = 0
        for block in inputFile.getBlockList():
            resource = Resource(memory, cpu, disk, network)
            job.addTask(IOTask(jobID + "-mapTask-" + str(blockIndex), Priority.NORMAL, resource, block))
            blockIndex += 1
        
        return job
    
    
    @staticmethod
    def genMapReduceJob(jobID, inputFile, mapMemory, mapCPU, mapDisk, mapNetwork, numOfReduce, redMemory, redCPU, redDisk, redNetwork, submissionTime):
        mapTaskList = []
        reduceTaskList = []
        mapIndex = 0
        for block in inputFile.getBlockList():
            mapResRequest = Resource(mapMemory, mapCPU, mapDisk, mapNetwork)
            mapTaskList.append(IOTask(jobID + "-mapTask-" + str(mapIndex), Priority.NORMAL, mapResRequest, block))
            mapIndex += 1
        
        for i in range(numOfReduce):
            reduceResRequest = Resource(redMemory, redCPU, redDisk, redNetwork)
            reduceTask = IOTask(jobID + "-redTask-" + str(i), Priority.HIGH, reduceResRequest, 
                                FileBlock(jobID + "tmp" + str(i), inputFile.getFileSize() / float(numOfReduce), BlockType.INTERMEDIATE))
            for mapTask in mapTaskList:
                mapTask.addChild(reduceTask)
            reduceTaskList.append(reduceTask)
            
        job = Job(jobID, submissionTime)
        
        for mapTask in mapTaskList:
            job.addTask(mapTask)
        for reduceTask in reduceTaskList:
            job.addTask(reduceTask)
            
        return job
        
        