#_*_coding: utf-8_*_

from numpy import *
import  operator

def createDataSet():
    """
    用户自定义测试的数据
    :return:
    """
    group = array([[1.0, 1.1], [1.0, 1.0], [0.0, 0.0], [0, 0.1], [5, 10], [10, 5]])
    labels = ["A", "A", "B", "B", "C", "C"]
    return  group, labels

def createDataTest():
    tGroup = array([[1.1, 1.0], [1.1, 1.2], [0.1, 0.0], [0.1, 0.1], [5, 9], [9.5, 5]])
    tLabels = ["A", "A", "B", "B","C", "C"]
    return  tGroup, tLabels


def autoNorm(dataSet):
    """
    按行处理，归一化
    :param dataset:
    :return:
    """
    minX = dataSet.min(0)
    maxX = dataSet.max(0)
    ranges = maxX - minX
    normDatSet = zeros(shape(dataSet))
    m = dataSet.shape[0]
    normDatSet = (dataSet - tile(minX, (m, 1)))/tile(ranges, (m, 1))
    return  normDatSet

def classify(inX, dataSet, labels, k):
    """
    用于输入的向量inX,输入训练样本集dataset，标签向量labels,邻居数k
    :param inX:
    :param dataSet:
    :param labels:
    :param k:
    :return:
    """
    dataSetSize = dataSet.shape[0] #shape函数获取第一纬度的长度
    sqDiffMat = (tile(inX, (dataSetSize, 1)) - dataSet)**2 #取差的平方
    distances = (sqDiffMat.sum(axis=1))**0.5
    sortD = distances.argsort()
    classCount = {}
    for i in range(k):
        voteLabel = labels[sortD[i]]
        classCount[voteLabel] = classCount.get(voteLabel,0)+1
    classSort = sorted(classCount.iteritems(), key=operator.itemgetter(1), reverse=True)
    return  classSort[0][0]


def classTest(dataSet, labels, tDataSet, tLabels):
    errorCount = 0.0
    m = dataSet.shape[0]
    for i in range(m):
        ressult = classify(tDataSet[i], dataSet, labels,6)
        if(ressult != tLabels[i]): errorCount+=1
    print("the total error tate is: %f" %(errorCount/float(m)))

group, labels = createDataSet()
group = autoNorm(group)
label = classify([0, 0], group, labels, 3)
print label

#错误测试
tGroup, tLabels = createDataTest()
tGroup = autoNorm(group)
classTest(group, labels, tGroup, tLabels)






