'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

from Resources import Resources

class FSSchedulerNode(object):
    '''
    classdocs
    '''


    def __init__(self, nodeID, resourceCapacity):
        '''
        Constructor
        '''
        self._nodeID = nodeID
        self._resourceCapacity = resourceCapacity
        self._availableResource = Resources.clone(resourceCapacity)
        self._usedResource = Resources.createResource(0, 0, 0, 0)
        self._blockList = []
        self._numContainers = 0
        self._reservedContainer = None
        self._reservedAppSchedulable = None
        self._launchedContainers = []
        self._diskConsumingTasks = []
        self._networkConsumingTasks = []
        
        
    def __str__(self):
        return str(self._nodeID)
    
    
    def getNodeID(self):
        return str(self._nodeID)
        
        
    def addDiskConsumingTask(self, task):
        self._diskConsumingTasks.append(task)
        
        
    def addNetworkConsumingTask(self, task):
        self._networkConsumingTasks.append(task)
        
        
    def removeDiskConsumingTask(self, task):
        self._diskConsumingTasks.remove(task)
        
        
    def removeNetworkConsumingTask(self, task):
        self._networkConsumingTasks.remove(task)
        
        
    def calDiskBandwidth(self):
        diskBandwidth = self._resourceCapacity.getDisk()
        weight = 0
        for task in self._diskConsumingTasks:
            weight += task.getResource().getDisk()
        
        bandwidth = 0
        for task in self._diskConsumingTasks:
            if weight == 0:
                bandwidth = 0
            else:
                bandwidth = float(diskBandwidth) * task.getResource().getDisk() / weight
            if task.getAllocatedNode() == self:
                task.setLocalDiskBandwidth(bandwidth)
            else:
                task.setRemoteDiskBandwidth(bandwidth)
                
                
    def calNetworkBandwidth(self):
        networkBandwidth = self._resourceCapacity.getNetwork()
        weight = 0
        for task in self._networkConsumingTasks:
            weight += task.getResource().getNetwork()
            
        bandwidth = 0
        for task in self._networkConsumingTasks:
            if weight == 0:
                bandwidth = 0
            else:
                bandwidth = float(networkBandwidth) * task.getResource().getNetwork() / weight
            if task.getAllocatedNode() == self:
                task.setLocalNetworkBandwidth(bandwidth)
            else:
                task.setRemoteNetworkBandwidth(bandwidth)
        
        
    def getAvailableResource(self):
        return self._availableResource
    
    
    def getCapacity(self):
        return self._resourceCapacity
    
    
    def getUsedResource(self):
        return self._usedResource
        
        
    def uploadFileBlock(self, block):
        self._blockList.append(block)
        
    
    def getReservedContainer(self):
        return self._reservedContainer
    
    
    def getReservedAppSchedulable(self):
        return self._reservedAppSchedulable
    
    
    def getRunningContainers(self):
        return self._launchedContainers
    
    
    def deductAvailableResource(self, container):
        resource = container.getTask().getResource()
        if container.getTask().getExpectedNode() == None:
            Resources.subtractFrom(self._availableResource, resource)
            Resources.addTo(self._usedResource, resource)
        else:
            if container.getTask().getExpectedNode() == self:
                localResource = Resources.createResource(resource.getMemory(), resource.getCPU(), resource.getDisk(), 0)
                Resources.subtractFrom(self._availableResource, localResource)
                Resources.addTo(self._usedResource, localResource)
            else:
                localResource = resource
                Resources.subtractFrom(self._availableResource, localResource)
                Resources.addTo(self._usedResource, localResource)
                remoteResource = Resources.createResource(0, 0, resource.getDisk(), resource.getNetwork())
                remoteNode = container.getTask().getExpectedNode()
                Resources.subtractFrom(remoteNode.getAvailableResource(), remoteResource)
                Resources.addTo(remoteNode.getUsedResource(), remoteResource)
                
                
    def addAvailableResource(self, container):
        resource = container.getTask().getResource()
        if container.getTask().getExpectedNode() == None:
            Resources.addTo(self._availableResource, resource)
            Resources.subtractFrom(self._usedResource, resource)
        else:
            if container.getTask().getExpectedNode() == self:
                localResource = Resources.createResource(resource.getMemory(), resource.getCPU(), resource.getDisk(), 0)
                Resources.addTo(self._availableResource, localResource)
                Resources.subtractFrom(self._usedResource, localResource)
            else:
                localResource = resource
                Resources.addTo(self._availableResource, localResource)
                Resources.subtractFrom(self._usedResource, localResource)
                remoteResource = Resources.createResource(0, 0, resource.getDisk(), resource.getNetwork())
                remoteNode = container.getTask().getExpectedNode()
                Resources.addTo(remoteNode.getAvailableResource(), remoteResource)
                Resources.subtractFrom(remoteNode.getUsedResource(), remoteResource)
    
    
    def allocateContainer(self, container):
        self.deductAvailableResource(container)
        self._numContainers += 1
        self._launchedContainers.append(container)

    
    def releaseContainer(self, container):
        self._launchedContainers.remove(container)
        self._numContainers -= 1
        self.addAvailableResource(container)
        
        
        