'''
Created on Jan 8, 2015

@author: niuzhaojie
'''


class RMContainerInfo(object):
    '''
    classdocs
    '''


    def __init__(self, containerID, application, node, task, startTime):
        '''
        Constructor
        '''
        self._containerID = containerID
        self._application = application
        self._node = node
        self._task = task
        self._startTime = startTime
        self._finishTime = self._startTime
        
        
    def getTask(self):
        return self._task
    
    
    def getApplication(self):
        return self._application
    
    
    def getNode(self):
        return self._node
        
        
    def setFinishTime(self, finishTime):
        self._finishTime = finishTime
        