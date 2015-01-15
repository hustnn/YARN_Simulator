'''
Created on Jan 8, 2015

@author: niuzhaojie
'''
import sys

sys.path.append('../')

from ResourceType import ResourceType

class ComputeFairShares(object):
    '''
    classdocs
    '''
    COMPUTE_FAIR_SHARES_ITERATIONS = 25


    @staticmethod
    def getResourceValue(resource, resourceType):
        if resourceType == ResourceType.MEMORY:
            return resource.getMemory()
        elif resourceType == ResourceType.CPU:
            return resource.getCPU()
        else:
            raise Exception("invalide resource type")
        
        
    @staticmethod
    def setResourceValue(val, resource, resourceType):
        if resourceType == ResourceType.MEMORY:
            resource.setMemory(val)
        elif resourceType == ResourceType.CPU:
            resource.setCPU(val)
        else:
            raise Exception("invalid resource type")
    
    
    @staticmethod
    def resourceUsedWithWeightToResourceRatio(w2rRatio, schedulables):
        resourcesTaken = 0
        for sched in schedulables:
            resourcesTaken += sched.getWeight() * w2rRatio
        return resourcesTaken
        

    @classmethod
    def computeShares(cls, schedulables, totalResources, resourceType):
        if len(schedulables) == 0:
            return 
        
        totalResource = ComputeFairShares.getResourceValue(totalResources, resourceType)
        
        rMax = 1.0
        rUW2ResourceRatio = 0
        while True:
            rUW2ResourceRatio = ComputeFairShares.resourceUsedWithWeightToResourceRatio(rMax, schedulables)
            if rUW2ResourceRatio == 0 or rUW2ResourceRatio >= totalResource:
                break
            rMax *= 2.0
        
        left = 0
        right = rMax
        for i in range(cls.COMPUTE_FAIR_SHARES_ITERATIONS):
            mid = (left + right) / 2.0
            if ComputeFairShares.resourceUsedWithWeightToResourceRatio(mid, schedulables) < totalResource:
                left = mid
            else:
                right = mid
                
        for sched in schedulables:
            share = sched.getWeight() * right
            ComputeFairShares.setResourceValue(share, sched.getFairShare(), resourceType)
        