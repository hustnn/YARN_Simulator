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
        