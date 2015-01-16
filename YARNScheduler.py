'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from FSParentQueue import FSParentQueue
from FSLeafQueue import FSLeafQueue
from policies.PolicyParser import PolicyParser

class YARNScheduler(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self._consideringIO = True
        self._rootQueue = FSParentQueue("root", None, self)
        self._rootQueue.setPolicy(PolicyParser.getInstance("fair"))
        self._queues = {"root": self._rootQueue}
        
        
    def createQueue(self, queueName, maxApps, policy, isLeaf, parentQueueName):
        parentQueue = self._queues.get(parentQueueName)
        if isLeaf:
            queue = FSLeafQueue(queueName, parentQueue, self)
        else:
            queue = FSParentQueue(queueName, parentQueue, self)
            
        if parentQueue != None:
            parentQueue.addChildQueue(queue)
            
        queue.setMaxApps(maxApps)
        queue.setPolicy(PolicyParser.getInstance(policy))
        
        
    def consideringIO(self):
        return self._consideringIO
    
    
    def addApplication(self, job, queueName):
        