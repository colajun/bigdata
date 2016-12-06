#_*_ coding: utf-8_*_
from numpy import *
import  numpy as np
import  pandas as pd
import  matplotlib.pyplot as plt

df = pd.read_csv("/home/hadoop/bigdata/workspacepython/bigdata/tes1/machinelearing/test.txt")
def calcute_w():
    df1 = df[df.label==1]
    df2 = df[df.label==0]
    x1 = df1.values[:,1:3]
    x0 = df2.values[:, 1:3]
    mean1 = array([mean(x1[:,0]), mean(x1[:,1])])
    mean0 = array([mean(x0[:,0]), mean(x0[:, 1])])
    m1 = shape(x1)[0]
    sw = zeros(shape=(2, 2))
    for i in range(m1):
        xsmean = mat(x1[i,:]-mean1)
        sw+= xsmean.transpose()*xsmean

    m0 = shape(x0)[0]
    for i in range(m0):
        xsmean = mat(x0[i, :]-mean0)
        sw += xsmean.transpose()*xsmean
    w =(mean0 - mean1)*(mat(sw).I)
    return  w


def plot(w):
    dataMat = array(df[["density", "ratio_sugar"]].values[:,:])
    labelMat = mat(df["label"].values[:]).transpose()
    m = shape(dataMat)[0]
    xcord1 = []
    ycord1= []
    xcord2 = []
    ycord2 = []
    for i in range(m):
        if labelMat[i] ==1:
            xcord1.append(dataMat[i, 0])
            ycord1.append(dataMat[i,1])
        else:
            xcord2.append(dataMat[i, 0])
            ycord2.append(dataMat[i, 1])
    plt.figure(1)
    ax = plt.subplot(111)
    ax.scatter(xcord1, ycord1, s=30, c="red", marker="s")
    ax.scatter(xcord2, ycord2, s=30, c ="green")
    x= arange(-0.2, 0.8, 0.1)
    y = array((-w[0,0]*x)/w[0,1])
    print shape(x)
    print shape(y)
    plt.sca(ax)
    plt.plot(x, y)
    plt.xlabel("density")
    plt.ylabel("ratio_sugar")
    plt.title("LDA")
    plt.show()


w = calcute_w()
print w
plot(w)
















