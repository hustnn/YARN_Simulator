'''
Created on Jan 11, 2015

@author: zhaojie
'''

from Resource import Resource

class Resources(object):
    '''
    classdocs
    '''
    NONE = Resource(0, 0, 0, 0)
    
    @staticmethod
    def createResource(memory, cpu, disk, network):
        return Resource(memory, cpu, disk, network)
    
    
    @staticmethod
    def clone(resource):
        return Resource(resource.getMemory(), resource.getCPU(), resource.getDisk(), resource.getNetwork())
    
    
    @staticmethod
    def addTo(lhs, rhs):
        lhs.setMemory(lhs.getMemory() + rhs.getMemory())
        lhs.setCPU(lhs.getCPU() + rhs.getCPU())
        lhs.setDisk(lhs.getDisk() + rhs.getDisk())
        lhs.setNetwork(lhs.getNetwork() + rhs.getNetwork())
        return lhs
    
    
    @staticmethod
    def subtractFrom(lhs, rhs):
        lhs.setMemory(lhs.getMemory() - rhs.getMemory())
        lhs.setCPU(lhs.getCPU() - rhs.getCPU())
        lhs.setDisk(lhs.getDisk() - rhs.getDisk())
        lhs.setNetwork(lhs.getNetwork() - rhs.getNetwork())
        return lhs
    
    
    @staticmethod
    def fitsIn(smaller, bigger):
        return smaller.getMemory() <= bigger.getMemory() and smaller.getCPU() <= bigger.getCPU()
    
    
    @staticmethod
    def allFitIn(smaller, bigger):
        return smaller.getMemory() <= bigger.getMemory() and \
            smaller.getCPU() <= bigger.getCPU() and \
            smaller.getDisk() <= bigger.getDisk() and \
            smaller.getNetwork() <= bigger.getNetwork()
            
            
    @staticmethod
    def localFitIn(request, local):
        return request.getMemory() <= local.getMemory() and \
            request.getCPU() <= local.getCPU() and \
            request.getDisk() <= local.getDisk()
            
            
    @staticmethod
    def remoteFitIn(request, local, remote):
        return request.getMemory() <= local.getMemory() and \
            request.getCPU() <= local.getCPU() and \
            request.getDisk() <= local.getDisk() and \
            request.getNetwork() <= local.getNetwork() and \
            request.getDisk() <= remote.getDisk() and \
            request.getNetwork() <= remote.getNetwork()
    
    
    @classmethod
    def none(cls):
        return cls.NONE 
    
    
    @staticmethod
    def equals(lhs, rhs):
        return lhs == rhs
        