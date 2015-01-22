'''
Created on Jan 8, 2015

@author: niuzhaojie
'''

class Resource(object):
    '''
    classdocs
    '''


    def __init__(self, memory, cpu, disk, network):
        '''
        Constructor
        '''
        self._memory = memory
        self._cpu = cpu
        self._disk = disk
        self._network = network
        
        
    def __eq__(self, obj):
        return self._memory == obj.getMemory() and self._cpu == obj.getCPU() and self._disk == obj.getDisk() and self._network == obj.getNetwork()
    
    
    def __str__(self):
        return "<" + str(self._memory) + "," + str(self._cpu) + "," + str(self._disk) + "," + str(self._network) + ">"
        
    
    def setMemory(self, memory):
        self._memory = memory
        
        
    def setCPU(self, cpu):
        self._cpu = cpu
        
        
    def setDisk(self, disk):
        self._disk = disk
        
        
    def setNetwork(self, network):
        self._network = network
        
        
    def getMemory(self):
        return self._memory
    
    
    def getCPU(self):
        return self._cpu
    
    
    def getDisk(self):
        return self._disk
    
    
    def getNetwork(self):
        return self._network
    