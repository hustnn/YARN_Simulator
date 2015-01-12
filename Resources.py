'''
Created on Jan 11, 2015

@author: zhaojie
'''

from Resource import Resource

class Resources(object):
    '''
    classdocs
    '''
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
        