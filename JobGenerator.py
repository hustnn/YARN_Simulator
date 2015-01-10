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

class JobGenerator(object):
    '''
    classdocs
    '''
    @staticmethod
    def genComputeIntensitveJob(jobID, numOfTask, resource, execTime):
        job = Job(jobID)
        for i in range(numOfTask):
            job.addTask(ComputeTask(jobID + "-compute-" + str(i), Priority.NORMAL, resource, execTime))
            
        return job
    
    
    @staticmethod
    def genMapOnlyJob(jobID, inputFile, resource):
        job = Job(jobID)
        blockIndex = 0
        for block in inputFile._blockList:
            job.addTask(IOTask(jobID + "-map-" + str(blockIndex), Priority.NORMAL, resource, block))
            blockIndex += 1
        
        return job
    
    
    @staticmethod
    def genMapReduceJob(jobID, inputFile, mapResRequest, numOfReduce, reduceResRequest):
        mapTaskList = []
        reduceTaskList = []
        mapIndex = 0
        for block in inputFile._blockList:
            mapTaskList.append(IOTask(jobID + "-map-" + str(mapIndex), Priority.NORMAL, mapResRequest, block))
            mapIndex += 1
        
        for i in range(numOfReduce):
            reduceTask = IOTask(jobID + "-red-" + str(i), Priority.HIGH, reduceResRequest, 
                                FileBlock(jobID + "tmp" + str(i), inputFile._size / float(numOfReduce), BlockType.INTERMEDIATE))
            for mapTask in mapTaskList:
                mapTask.addChild(reduceTask)
            reduceTaskList.append(reduceTask)
        job = Job(jobID)
        for mapTask in mapTaskList:
            job.addTask(mapTask)
        for reduceTask in reduceTaskList:
            job.addTask(reduceTask)
            
        return job
        
        