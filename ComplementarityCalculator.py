'''
Created on Sep 24, 2015

@author: hustnn
'''

import math


def isVectorComplementary(vec1, vec2):
    if vec1.index(max(vec1)) == vec2.index(max(vec2)):
        return False
    else:
        return True
    
    
def vectorLen(v):
    s = 0
    for i in v:
        s += i * i
        
    return math.sqrt(s)


def dotProduct(v1, v2):
    s = 0
    for i in range(len(v1)):
        s += v1[i] * v2[i]
        
    return s

    
def calConsin(vec1, vec2):
    return float(dotProduct(vec1, vec2)) / (vectorLen(vec1) * vectorLen(vec2))


# the smaller, the more complementary
def calComplementarityOfTwoDVectors(v1, v2):
    if not isVectorComplementary(v1, v2):
        return calConsin([1, 1], [1, 1])
    else:
        diffV1 = calConsin(v1, [1, 1])
        diffV2 = calConsin(v2, [1, 1])
        return max(diffV1, diffV2)
    
    
def genVectorList():
    return [[0, 10], [1, 9], [2, 8], [3, 7], [4, 6], [5, 5], [6, 4], [7, 3], [8, 2], [9, 1], [10, 0]]


def sequentialPacking(vectorList1, vectorList2, clusterCapacity):
    clusterNum = 0
    curCap = list(clusterCapacity)
    for v in vectorList1:
        if curCap[0] > v[0] and curCap[1] > v[1]:
            curCap[0] -= v[0]
            curCap[1] -= v[1]
        else:
            clusterNum += 1
            curCap = list(clusterCapacity)
            curCap[0] -= v[0]
            curCap[1] -= v[1]
            
    for v in vectorList2:
        if curCap[0] >= v[0] and curCap[1] >= v[1]:
            curCap[0] -= v[0]
            curCap[1] -= v[1]
        else:
            clusterNum += 1
            curCap = list(clusterCapacity)
            curCap[0] -= v[0]
            curCap[1] -= v[1]
    
    clusterNum += 1
    return clusterNum
    
    
def efficientPacking(vectorList1, vectorList2, clusterCapacity):
    clusterNum = 0
    curCap = list(clusterCapacity)
    index1 = 0
    index2 = 0
    while (index1 < len(vectorList1) and index2 < len(vectorList2)):
        if  vectorLen(curCap) == 0:
            clusterNum += 1
            curCap = list(clusterCapacity) 
            
        if calConsin(vectorList1[index1], curCap) > calConsin(vectorList2[index2], curCap):
            c = vectorList1[index1]
            index1 += 1
        else:
            c = vectorList2[index2]
            index2 += 1
        if curCap[0] >= c[0] and curCap[1] >= c[1]:
            curCap[0] -= c[0]
            curCap[1] -= c[1]
        else:
            clusterNum += 1
            curCap = list(clusterCapacity)
            curCap[0] -= c[0]
            curCap[1] -= c[1]
    
    if (index1 < len(vectorList1)):
        for v in vectorList1[index1::]:
            if curCap[0] >= v[0] and curCap[1] >= v[1]:
                curCap[0] -= v[0]
                curCap[1] -= v[1]
            else:
                clusterNum += 1
                curCap = list(clusterCapacity)
                curCap[0] -= v[0]
                curCap[1] -= v[1]
    else:
        for v in vectorList2[index2::]:
            if curCap[0] >= v[0] and curCap[1] >= v[1]:
                curCap[0] -= v[0]
                curCap[1] -= v[1]
            else:
                clusterNum += 1
                curCap = list(clusterCapacity)
                curCap[0] -= v[0]
                curCap[1] -= v[1]
                
    clusterNum += 1
    return clusterNum
    

if __name__ == '__main__':
    clusterCapacity = [100, 100]
    vectorList = genVectorList()
    print("vector1\tvector2\tcomplementaryt\timprovement")
    for vector1 in vectorList:
        for vector2 in vectorList:
            # the larger, the better (more complementary)
            complementarity = '%.2f' % float(1 - calComplementarityOfTwoDVectors(vector1, vector2))
            vectorList1 = [vector1] * 100
            vectorList2 = [vector2] * 100
            seqCost = sequentialPacking(vectorList1, vectorList2, clusterCapacity)
            efficientCost = efficientPacking(vectorList1, vectorList2, clusterCapacity)
            improvement = '%.2f' % float(1 - float(efficientCost) / seqCost)
            print(str(vector1) + "\t" + str(vector2) + "\t" + complementarity + "\t" + improvement)
    