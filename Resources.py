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
    
    
    @classmethod
    def none(cls):
        return cls.NONE 
    
    
    @staticmethod
    def equals(lhs, rhs):
        return lhs == rhs
        