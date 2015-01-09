'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from Task import Task

class ComputeTask(Task):
    '''
    classdocs
    '''


    def __init__(self, taskID, priority, resourceRequest, execTime):
        '''
        Constructor
        '''
        super().__init__(taskID, priority, resourceRequest)
        self._execTime = execTime
        