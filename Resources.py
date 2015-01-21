'''
Created on Jan 11, 2015

@author: zhaojie
'''

from Resource import Resource
import math

class Resources(object):
    '''
    classdocs
    '''
    NONE = Resource(0, 0, 0, 0)
    NOTFIT = Resource(-1, -1, -1 ,-1)
    
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
    
    
    @classmethod
    def notFit(cls):
        return cls.NOTFIT
    
    
    @staticmethod
    def equals(lhs, rhs):
        return lhs == rhs
    
    
    @staticmethod
    def normalizedDotProduct(r1, r2, capacity):
        m1 = float(r1.getMemory()) / capacity.getMemory()
        c1 = float(r1.getCPU()) / capacity.getCPU()
        m2 = float(r2.getMemory()) / capacity.getMemory()
        c2 = float(r2.getCPU()) / capacity.getCPU()
        return m1 * m2 + c1 * c2
    
    
    @staticmethod
    def cosineSimilarity(r1, r2):
        dotProduct = r1.getMemory() * r2.getMemory() + r1.getCPU() * r2.getCPU()
        len1 = math.sqrt(math.pow(r1.getMemory(), 2) + math.pow(r1.getCPU(), 2))
        len2 = math.sqrt(math.pow(r2.getMemory(), 2) + math.pow(r2.getCPU(), 2))
        
        if len1 == 0 or len2 == 0:
            return 0.0
        else:
            return float(dotProduct) / (len1 * len2)
        