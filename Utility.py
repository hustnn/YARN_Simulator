'''
Created on Jan 16, 2015

@author: zhaojie
'''

import numpy
from numpy import array
from scipy.cluster.vq import vq, kmeans, whiten, kmeans2

from Resources import Resources

import math

class Utility(object):
    '''
    classdocs
    '''

    @staticmethod
    def sign(num):
        if num == 0:
            return 0
        elif num > 0:
            return 1
        else:
            return -1
        
    
    @staticmethod
    def compareTo(str1, str2):
        if str1 == str2:
            return 0
        elif len(str1) < len(str2):
            return -1
        elif len(str1) > len(str2):
            return 1
        else:
            if str1 < str2:
                return -1
            else:
                return 1
            
    
    @classmethod
    def calEntropyOfWorkload(cls, apps, vectorQuantinationNum):
        resVectorList = []
        for app in apps:
            resourceRequest = app.getCurrentResourceDemand()
            if not Resources.equals(resourceRequest, Resources.none()):
                resVector = resourceRequest.getResourceVector()
                resVectorList.append(resVector)
        
        if (len(resVectorList) == 0):
            return 0
                
        resVectorArray = numpy.asarray(resVectorList)
        r, d = kmeans(resVectorArray, vectorQuantinationNum, 1000)
        (code, distor) = vq(resVectorArray, r)
        #print(code)
        distributions = {}
        for n in code:
            if n in distributions.keys():
                distributions[n] = distributions[n] + 1
            else:
                distributions[n] = 1
        totalCount = len(code)
        probDis = {}
        for k, v in distributions.items():
            probDis[k] = float(v) / totalCount
        
        return cls.calEntropy(probDis)
    
    
    @classmethod
    def calEntropyOfVectorList(cls, vectorList, vectorQuantinationNum = 4):  
        #print(vectorList)  
        #dict4 = {(6, 1, 1, 1): 0, (2, 1, 1, 1): 0, (1, 6, 1, 1): 1, (1, 2, 1, 1): 1, (1, 1, 6, 1): 2, (1, 1, 2, 1): 2, (1, 1, 1, 6): 3, (1, 1, 1, 2): 3}
        #dict8 = {(6, 1, 1, 1): 0, (3, 1, 1, 1): 1, (1, 6, 1, 1): 2, (1, 3, 1, 1): 3, (1, 1, 6, 1): 4, (1, 1, 3, 1): 5, (1, 1, 1, 6): 6, (1, 1, 1, 3): 7}
        code = vectorList
        
        '''resVectorArray = numpy.asarray(vectorList)
        r, d = kmeans(resVectorArray, vectorQuantinationNum, 1000)
        (code, distor) = vq(resVectorArray, r)'''
        
        #print(code)
        '''code = []
        for v in vectorList:
            if vectorQuantinationNum == 4:
                code.append(dict4[tuple(v)])
            else:
                code.append(dict8[tuple(v)])'''
        #print(code)
        
        distributions = {}
        for n in code:
            if n in distributions.keys():
                distributions[n] = distributions[n] + 1
            else:
                distributions[n] = 1
        
        totalCount = len(code)
        probDis = {}
        for k, v in distributions.items():
            probDis[k] = float(v) / totalCount
        return cls.calEntropy(probDis)
    
    
    @classmethod
    def calEntropyOfResourceVectorList(cls, vectorList, vectorQuantinationNum = 4):  
        #print(vectorList)  
        #dict4 = {(6, 1, 1, 1): 0, (2, 1, 1, 1): 0, (1, 6, 1, 1): 1, (1, 2, 1, 1): 1, (1, 1, 6, 1): 2, (1, 1, 2, 1): 2, (1, 1, 1, 6): 3, (1, 1, 1, 2): 3}
        #dict8 = {(6, 1, 1, 1): 0, (3, 1, 1, 1): 1, (1, 6, 1, 1): 2, (1, 3, 1, 1): 3, (1, 1, 6, 1): 4, (1, 1, 3, 1): 5, (1, 1, 1, 6): 6, (1, 1, 1, 3): 7}
        code = []
        #mapping normalized resource vector list to category
        for resVector in vectorList:
            code.append(cls.calEntropy(resVector))
        
        '''
        resToCodeMappint = {(6, 1, 1, 1): 1, (1, 6, 1, 1): 2, (1, 1, 6, 1): 3, (1, 1, 1, 6): 4}
        for v in vectorList:
            code.append(resToCodeMappint[tuple(v)])'''
        
        '''resVectorArray = numpy.asarray(vectorList)
        r, d = kmeans(resVectorArray, vectorQuantinationNum, 1000)
        (code, distor) = vq(resVectorArray, r)'''
        
        #print(code)
        '''code = []
        for v in vectorList:
            if vectorQuantinationNum == 4:
                code.append(dict4[tuple(v)])
            else:
                code.append(dict8[tuple(v)])'''
        #print(code)
        
        distributions = {}
        for n in code:
            if n in distributions.keys():
                distributions[n] = distributions[n] + 1
            else:
                distributions[n] = 1
        
        totalCount = len(code)
        probDis = {}
        for k, v in distributions.items():
            probDis[k] = float(v) / totalCount
        return cls.calEntropy(probDis)
    
    
    @staticmethod
    def calEntropy(probDict):
        s = 0
        for p in probDict.values(): 
            if p > 0:
                s += p * math.log(float(1) / p) / math.log(2)
        
        return s
    
    
    @staticmethod
    def getIndexOfDominantResource(normalizedResVector):
        return normalizedResVector.index(max(normalizedResVector))