'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from Task import Task
from SchedulableStatus import SchedulableStatus

class IOTask(Task):
    '''
    classdocs
    '''


    def __init__(self, taskID, priority, resource, block):
        '''
        Constructor
        '''
        super(IOTask, self).__init__(taskID, priority, resource)
        self._block = block
        self._expectedNode = self._block._location
        self._workload = self._block.getBlockSize()
        
        
    def getWorkload(self):
        return self._workload
        
        
    def setLocalDiskBandwidth(self, disk):
        self._localDiskBandwidth = disk
        
        
    def setLocalNetworkBandwidth(self, network):
        self._localNetworkBandwidth = network
        
        
    def setRemoteDiskBandwidth(self, disk):
        self._remoteDiskBandwidth = disk
        
        
    def setRemoteNetworkBandwidth(self, network):
        self._remoteNetworkBandwidth = network
        
        
    def schedule(self, t):
        if self._status == SchedulableStatus.RUNNING:
            if self._expectedNode == None:
                speed = min(self._localNetworkBandwidth, self._localDiskBandwidth)
                #print("any: " + str(speed) + ", allocated node: " + str(self._scheduledNode))
            else:
                if self._expectedNode == self._scheduledNode:
                    speed = speed = self._localDiskBandwidth / 2.0
                    #print("local: " + str(speed) + ", allocated node: " + str(self._scheduledNode))
                else:
                    speed =   speed = min(self._remoteDiskBandwidth, 
                                          self._remoteNetworkBandwidth, 
                                          self._localNetworkBandwidth, 
                                          self._localDiskBandwidth)
                    #print("remote:" + str(speed) + ", allocated node: " + str(self._scheduledNode) + ", expected node: " + str(self._expectedNode))
                    
            
            if self._workload <= t * speed:
                self._workload = 0
            else:  
                self._workload -= t * speed
            
            