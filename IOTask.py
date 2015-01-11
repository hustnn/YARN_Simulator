'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
from Task import Task
from BlockType import BlockType
from SchedulableStatus import SchedulableStatus

class IOTask(Task):
    '''
    classdocs
    '''


    def __init__(self, taskID, priority, resourceRequest, block):
        '''
        Constructor
        '''
        super(IOTask, self).__init__(taskID, priority, resourceRequest)
        self._block = block
        self._remoteNode = None
        self._localNode = None
        self._workload = block._size
        self._remoteDiskBandwidth = 0
        self._remoteNetworkBandwidth = 0
        self._localDiskBandwidth = 0
        self._localNetworkBandwidth = 0
        
    
    def getRemoteFlag(self):
        return self._remoteNode != None and self._localNode != None
    
    
    def getLocalDiskFlag(self):
        return self._remoteNode == None and self._localNode != None and self._block._location == self._localNode
    
    
    def getSimpleShuffleFlag(self):
        return self._remoteNode == None and self._localNode != None and self._block._type == BlockType.INTERMEDIATE
    
    
    def updateRemoteDiskBandwidth(self, disk):
        self._remoteDiskBandwidth = disk
        
        
    def updateRemoteNetworkBandwidth(self, network):
        self._remoteNetworkBandwidth = network
        
        
    def updateLocalDiskBandwidth(self, disk):
        self._localDiskBandwidth = disk
        
        
    def updateLocalNetworkBandwidth(self, network):
        self._localNetworkBandwidth = network
        
        
    def schedule(self, t):
        if self._status == SchedulableStatus.RUNNING:
            if self.getRemoteFlag():
                speed = min(self._remoteDiskBandwidth, self._remoteNetworkBandwidth, self._localNetworkBandwidth, self._localDiskBandwidth)
            elif self.getLocalDiskFlag():
                speed = self._localDiskBandwidth / 2.0
            elif self.getSimpleShuffleFlag():
                speed = min(self._localNetworkBandwidth, self._localDiskBandwidth)
            else:
                speed = 0
                
            self._workload -= t * speed
            
            