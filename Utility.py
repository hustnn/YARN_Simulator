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
    def calEntropyOfVectorList(cls, codes, vectorQuantinationNum = 4):  
        #print(vectorList)  
        #dict4 = {(6, 1, 1, 1): 0, (2, 1, 1, 1): 0, (1, 6, 1, 1): 1, (1, 2, 1, 1): 1, (1, 1, 6, 1): 2, (1, 1, 2, 1): 2, (1, 1, 1, 6): 3, (1, 1, 1, 2): 3}
        #dict8 = {(6, 1, 1, 1): 0, (3, 1, 1, 1): 1, (1, 6, 1, 1): 2, (1, 3, 1, 1): 3, (1, 1, 6, 1): 4, (1, 1, 3, 1): 5, (1, 1, 1, 6): 6, (1, 1, 1, 3): 7}
        
        distributions = {}
        for n in codes:
            if n in distributions.keys():
                distributions[n] = distributions[n] + 1
            else:
                distributions[n] = 1
        
        totalCount = len(codes)
        probDis = {}
        for k, v in distributions.items():
            probDis[k] = float(v) / totalCount
        return cls.calEntropy(probDis)
    
    
    @classmethod
    def calEntropyOfResourceVectorList(cls, vectorList, vectorQuantinationNum = 4):  
        code = []
        #mapping normalized resource vector list to category
        for resVector in vectorList:
            code.append(cls.getIndexOfDominantResource(resVector))
        
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
    
    
    @staticmethod
    def isVectorComplementary(vec1, vec2):
        if vec1.index(max(vec1)) == vec2.index(max(vec2)):
            return False
        else:
            return True
        
        
    @staticmethod
    def vectorLen(v):
        s = 0
        for i in v:
            s += i * i
            
        return math.sqrt(s)
    
    
    @staticmethod
    def dotProject(v1, v2):
        s = 0
        for i in range(len(v1)):
            s += v1[i] * v2[i]
            
        return s
    
    
    @classmethod
    def calConsin(cls, v1, v2):
        return float(cls.dotProject(v1, v2)) / (cls.vectorLen(v1) * cls.vectorLen(v2))
    
    
    #the larger, the more complementary(better)
    @classmethod
    def calComplementarityOfTwoDVectors(cls, v1, v2):
        if not cls.isVectorComplementary(v1, v2):
            return 0
        else:
            diffV1 = cls.calConsin(v1, [1, 1])
            diffV2 = cls.calConsin(v2, [1, 1])
            return 1 - max(diffV1, diffV2)
        
        
    @classmethod
    def calSymmetryOfTwoDVectors(cls, vectorList):
        nums = [0, 0]
        for vec in vectorList:
            index = vec.index(max(vec))
            nums[index] += 1
            
        return float(min(nums)) / max(nums)