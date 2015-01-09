'''
Created on Jan 9, 2015

@author: niuzhaojie
'''

class FileBlock(object):
    '''
    classdocs
    '''


    def __init__(self, blockID, size):
        '''
        Constructor
        '''
        self._blockID = blockID
        self._size = size
        self._location = None
        
        
    def assignToNode(self, node):
        self._location = node