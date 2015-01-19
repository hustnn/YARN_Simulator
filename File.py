'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
import Configuration
from FileBlock import FileBlock
from BlockType import BlockType

class File(object):
    '''
    classdocs
    '''


    def __init__(self, fileName, size):
        '''
        Constructor
        '''
        self._fileName = fileName
        self._size = size
        self._blockList = []
        blockNum = self._size / Configuration.BLOCK_SIZE
        for i in range(blockNum):
            self._blockList.append(FileBlock(fileName + str(i), Configuration.BLOCK_SIZE, BlockType.PERMANENT))
            
    
    def getFileName(self):
        return self._fileName
    
    
    def getFileSize(self):
        return self._size
    
    
    def getBlockList(self):
        return self._blockList