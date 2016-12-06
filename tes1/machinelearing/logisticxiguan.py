#_*_ coding:utf-8_*_

from numpy import  *
import  pandas as pd
import  matplotlib.pyplot as plt

#读取文件数据
# df = pd.read_csv('a.txt', sep=' ', header=None, dtype=str, na_filter=False)
names = ["idx", "density", "ratio_sugar", "label"]
df = pd.read_csv("/home/hadoop/bigdata/workspacepython/bigdata/tes1/machinelearing/test.txt")
m,n = shape(df)
df["norm"] = ones((m,1))
dataMat = array(df[["density", "ratio_sugar","norm"]].values[:,:])
labelMat = mat(df["label"].values[:]).transpose()


#sigmod函数
def sigmod(inX):
    return  1.0 /(1+exp(-inX))


#梯度上升算法
def gradAscent(dataMat, labelMat):
    m,n = shape(df.values)
    alpha = 0.1
    maxCycles = 500
    weights = array((ones(n, 1)))
    for k in range(maxCycles):
        a = dot(dataMat, weights)
        h = sigmod(a)
        error = (labelMat -h)
        weights = weights + alpha*dot(dataMat.transpose(), error)
    return  weights

#随机梯度上升
def randomgradAscent(dataMat, label, numIter=50):
    m,n = shape(dataMat)
    weights = ones(n)
    for j in range(numIter):
        dataIndex = range(m)
        for i in range(m):
            alpha = 40/(1.0+j+i)+0.2
            randIndex_Index = int(random.uniform(0, len(dataIndex)))
            randIndex = dataIndex[randIndex_Index]
            h = sigmod(sum(dot(dataMat[randIndex], weights)))
            error = (label[randIndex] -h)
            weights = weights +alpha*error[0, 0]*(dataMat[randIndex].transpose())
    return  weights



#画图
def plotBestFit(weights):
    m = shape(dataMat)[0]
    xcord1 = []
    ycord1 = []
    xcord2 = []
    ycord2 = []
    for i in range(m):
        if labelMat[i] ==1:
            xcord1.append(dataMat[i,1])
            ycord1.append(dataMat[i, 2])
        else:
            xcord2.append(dataMat[i, 1])
            ycord2.append(dataMat[i, 2])

    plt.figure(1)
    ax=plt.subplot(111)
    ax.scatter(xcord1, ycord1, s = 30, c="red")
    ax.scatter(xcord2, ycord2, s= 30, c="green")
    x = arange(0.2,0.8, 0.1)
    y = array((-weights[0]-weights[1]*x)/weights[2])

    print shape(x)
    print shape(y)
    plt.sca(ax)
    plt.plot(x, y)

    plt.xlabel("density")
    plt.ylabel("ratio_sugar")
    plt.title("random gradAscent logistic regression")
    plt.show()


# weights = randomgradAscent(dataMat, labelMat)
weights = gradAscent(dataMat, labelMat)
plotBestFit(weights)



















