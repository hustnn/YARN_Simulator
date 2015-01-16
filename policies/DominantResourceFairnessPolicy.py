'''
Created on Jan 8, 2015

@author: niuzhaojie
'''
import sys

sys.path.append('../')

from SchedulingPolicy import SchedulingPolicy
from ResourceWeights import ResourceWeights
from ResourceType import ResourceType
from Utility import Utility

class DominantResourceFairnessPolicy(SchedulingPolicy):
    '''
    classdocs
    '''
    NAME = "DRF"

    
    def compare(self, s1, s2):
        sharesOfCluster1 = ResourceWeights()
        sharesOfCluster2 = ResourceWeights()
        resourceOrder1 = [ResourceType.MEMORY, ResourceType.CPU]
        resourceOrder2 = [ResourceType.MEMORY, ResourceType.CPU]
        
        self.calculateShares(s1.getResourceUsage(), self._clusterCapacity, sharesOfCluster1, resourceOrder1, s1.getWeight())
        self.calculateShares(s2.getResourceUsage(), self._clusterCapacity, sharesOfCluster2, resourceOrder2, s2.getWeight())
        
        s1Needy = s1.getDemand().getMemory() > s1.getResourceUsage().getMemory()
        s2Needy = s2.getDemand().getMemory() > s2.getResourceUsage().getMemory()
        
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
        # sort order vector by resource share
        if resourceOrder != None:
            if shares.getWeight(ResourceType.MEMORY) > shares.getWeight(ResourceType.CPU):
                resourceOrder[0] = ResourceType.MEMORY
                resourceOrder[1] = ResourceType.CPU
            else:
                resourceOrder[0] = ResourceType.CPU
                resourceOrder[1] = ResourceType.MEMORY
                
                
    def compareShares(self, shares1, shares2, resourceOrder1, resourceOrder2):
        for i in range(len(resourceOrder1)):
            ret = Utility.sign(shares1.getWeight(resourceOrder1[i]) - shares2.getWeight(resourceOrder2[i]))
            if ret != 0:
                return ret
        return 0
            