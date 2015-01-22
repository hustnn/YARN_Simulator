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
    def greaterAtLeastOne(r1, r2):
        return r1.getMemory() > r2.getMemory() or r1.getCPU() > r2.getCPU() or r1.getDisk() > r2.getDisk() or r1.getNetwork() > r2.getNetwork()
    
    
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
    def normalizedDotProduct(r1, r2, capacity, considerIO = False):
        m1 = float(r1.getMemory()) / capacity.getMemory()
        c1 = float(r1.getCPU()) / capacity.getCPU()
        d1 = float(r1.getDisk()) / capacity.getDisk()
        n1 = float(r1.getNetwork()) / capacity.getNetwork()
        m2 = float(r2.getMemory()) / capacity.getMemory()
        c2 = float(r2.getCPU()) / capacity.getCPU()
        d2 = float(r2.getDisk()) / capacity.getDisk()
        n2 = float(r2.getNetwork()) / capacity.getNetwork()
        
        if not considerIO:
            return m1 * m2 + c1 * c2
        else:
            return m1 * m2 + c1 * c2 + d1 * d2 + n1 * n2
    
    
    @staticmethod
    def cosineSimilarity(r1, r2, considerIO = False):
        if not considerIO:
            dotProduct = r1.getMemory() * r2.getMemory() + r1.getCPU() * r2.getCPU()
            len1 = math.sqrt(math.pow(r1.getMemory(), 2) + math.pow(r1.getCPU(), 2))
            len2 = math.sqrt(math.pow(r2.getMemory(), 2) + math.pow(r2.getCPU(), 2))
        
            if len1 == 0 or len2 == 0:
                return 0.0
            else:
                return float(dotProduct) / (len1 * len2)
        else:
            dotProduct = r1.getMemory() * r2.getMemory() + r1.getCPU() * r2.getCPU() + r1.getDisk() * r2.getDisk() + r1.getNetwork() * r2.getNetwork()
            len1 = math.sqrt(math.pow(r1.getMemory(), 2) + math.pow(r1.getCPU(), 2) + math.pow(r1.getDisk(), 2) + math.pow(r1.getNetwork(), 2))
            len2 = math.sqrt(math.pow(r2.getMemory(), 2) + math.pow(r2.getCPU(), 2) + math.pow(r2.getDisk(), 2) + math.pow(r2.getNetwork(), 2))
        
            if len1 == 0 or len2 == 0:
                return 0.0
            else:
                return float(dotProduct) / (len1 * len2)
        