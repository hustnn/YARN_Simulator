'''
Created on Jan 21, 2015

@author: niuzhaojie
'''
import sys

sys.path.append('../')

from SchedulingPolicy import SchedulingPolicy
from ResourceWeights import ResourceWeights
from ResourceType import ResourceType
from Utility import Utility
from ComputeFairShares import ComputeFairShares
from Resources import Resources


class MultipleResourceFairnessPolicy(SchedulingPolicy):
    '''
    classdocs
    '''
    NAME = "PACKING"
    
    
    def getComparator(self):
        return self.compare
    
    
    def computeShares(self, schedulables, totalResources):
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.MEMORY)
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.CPU)
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.DISK)
        ComputeFairShares.computeShares(schedulables, totalResources, ResourceType.NETWORK)
        
        
    def compare(self, s1, s2):
        sharesOfCluster1 = ResourceWeights()
        sharesOfCluster2 = ResourceWeights()
        resourceOrder1 = [ResourceType.MEMORY, ResourceType.CPU, ResourceType.DISK, ResourceType.NETWORK]
        resourceOrder2 = [ResourceType.MEMORY, ResourceType.CPU, ResourceType.DISK, ResourceType.NETWORK]
        
        self.calculateShares(s1.getResourceUsage(), self._clusterCapacity, sharesOfCluster1, resourceOrder1, s1.getWeight())
        self.calculateShares(s2.getResourceUsage(), self._clusterCapacity, sharesOfCluster2, resourceOrder2, s2.getWeight())
        
        
        s1Needy = Resources.greaterAtLeastOne(s1.getDemand(), s1.getResourceUsage())
        s2Needy = Resources.greaterAtLeastOne(s2.getDemand(), s2.getResourceUsage())
        
        res = 0
        
        if s1Needy and not s2Needy:
            res = -1
        elif s2Needy and not s1Needy:
            res = 1
        elif s1Needy and s2Needy:
            res = self.compareShares(sharesOfCluster1, sharesOfCluster2, resourceOrder1, resourceOrder2)
            
        if res == 0:
            res = Utility.sign(s1.getStartTime() - s2.getStartTime())
            if res == 0:
                res = Utility.compareTo(s1.getName(), s2.getName())
                
        return res
        
        
    def calculateShares(self, resource, pool, shares, resourceOrder, weight):
        shares.setWeight(ResourceType.MEMORY, float(resource.getMemory()) / (pool.getMemory() * weight))
        shares.setWeight(ResourceType.CPU, float(resource.getCPU()) / (pool.getCPU() * weight))
        shares.setWeight(ResourceType.DISK, float(resource.getDisk()) / (pool.getDisk() * weight))
        shares.setWeight(ResourceType.NETWORK, float(resource.getNetwork()) / (pool.getNetwork() * weight))
        
        res = sorted(shares.getWeightsDict().items(), key = lambda x : x[1], reverse = True)
        if resourceOrder != None:
            for i in range(len(res)):
                resourceOrder[i] = res[i][0]
            
    
    def compareShares(self, shares1, shares2, resourceOrder1, resourceOrder2):
        for i in range(len(resourceOrder1)):
            ret = Utility.sign(shares1.getWeight(resourceOrder1[i]) - shares2.getWeight(resourceOrder2[i]))
            if ret != 0:
                return ret
        return 0
        