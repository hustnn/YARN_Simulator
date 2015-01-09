'''
Created on Jan 9, 2015

@author: niuzhaojie
'''
import Configuration
from FileBlock import FileBlock

class File(object):
    '''
    classdocs
    '''


    def __init__(self, fileID, fileName, size):
        '''
        Constructor
        '''
        self._fileID = fileID
        self._fileName = fileName
        self._size = size
        self._blockList = []
        blockNum = self._size / Configuration.BLOCK_SIZE
        for i in range(blockNum):
            self._blockList.append(FileBlock(i, Configuration.BLOCK_SIZE))