'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from Task import Task

class ComputeTask(Task):
    '''
    classdocs
    '''


    def __init__(self, taskID, priority, resource, execTime):
        '''
        Constructor
        '''
        super(ComputeTask, self).__init__(taskID, priority, resource)
        self._execTime = execTime
        self._workload = execTime
        
        
    def schedule(self, t):
        self._workload -= t
        