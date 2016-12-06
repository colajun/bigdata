#coding=utf-8
"""
Create on 2016-09-29 @author: zhujun
输入：打开ALL_BHSpider_Result.txt 对应1000个文本
001～400 5A景区 401～600动物 601~800 人物 801～1000 国家
输出： BHTfidf.txt tfidf值 聚类图形 1000个类标
参数: weight权重 这是一个重要的参数
"""
import  time
import  re
import  os
import  sys
import  shutil
import  numpy as np
import  scipy
import  matplotlib
import  codecs
import  matplotlib.pyplot as  plt
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import HashingVectorizer


if __name__ == "__main__":

    ##################################
    #      第一步 计算TFIDF
    #文档语料 空格连接
    corpus = []
    #读取语料 一行语料为一个文本
    for line in open("/run/media/hadoop/IR1_SSS_X64/python kmeans/01_All_BHSpider_Content_Result.txt", "r").readlines():
        #print line
        corpus.append(line.strip())
    #print corpus


    #参考: http://blog.csdn.net/abcjennifer/article/details/23615947
    #vectorizer = HashingVectorizer(n_features = 4000)

    #将文本中的词语转换为词频矩阵 矩阵元素a[i][j]表示j词在i类文本的词频
    vectorizer = CountVectorizer()

    #该类会统计每个词语的tf-idf权值
    transformer = TfidfTransformer()

    #第一个fit_transform是计算tf_idf 第二个fit_transform是将为文本转化为词频矩阵
    tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))

    #获取词带模型中的所有词语
    word = vectorizer.get_feature_names()

    #将tf-idf矩阵抽取出来,元素w[i][j]表示j词在i类文本中的tf-idf权重
    weight  = tfidf.toarray

    #打印特征向量的文本内容
    print "Features length: "+str(len(word))
    ressName = "/run/media/hadoop/IR1_SSS_X64/python kmeans/BHTfidf_Result.txt"
    result = codecs.open(ressName, "w", "utf-8")
    for j in range(len(word)):
        result.write(word[j]+" ")
    result.write("\r\n\r\n")

    #dayin每类文本的tf-idf词语权重， 第一个for遍历所有的文本，第二个for遍历某一类文本下的词语权重
    # for i in range(len(weight)):
    #     #print u"......这里输出",i, u"类文本的词语tf-idf权重----"
    #     for j in range(len(word)):
    #         result.write(str(weight[i][j])+" ")
    #     result.write("\r\n\r\n")
    result.close()

    ################################
    #                第二步 聚类Kmeans
    print "Start Kmeans:"
    from sklearn.cluster import KMeans
    clf = KMeans(n_clusters=4)
    s = clf.fit(weight)
    print s
    """
    print "Start MiniBatchKmeans"
    clf = MiniBatchKMeans(n_clusters=20)
    s = clf.fit(weight)
    print s
    """
    #中心点

    print (clf.cluster_centers_)

    #每个样本所属的簇
    label = []
    print (clf.labels_)
    i = 1
    while i <= len(clf.labels_):
        print i, clf.labels_[i-1]
        label.append(clf.labels_[i-1])
        i = i +1
    #用来评估簇是否合适，距离小说明簇分得好，选取临界点簇的个数 958.137281791
    print  (clf.inertia_)


    ####################################
    #            图形输出 降维

    from  sklearn.decomposition import  PCA
    pca = PCA(n_components=2)
    newData = pca.fit_transform(weight)
    print  newData

    #5A景区
    x1 = []
    y1 = []
    i =0
    while i < 400:
        x1.append(newData[i][0])
        y1.append(newData[i][1])
        i += 1

    # 动物
    x2 = []
    y2 = []
    i = 400
    while i < 600:
        x1.append(newData[i][0])
        y1.append(newData[i][1])
        i += 1

    #人物
    x3 = []
    y3 = []
    i = 600
    while i < 800:
        x3.append(newData[i][0])
        y3.append(newData[i][1])
        i += 1

    #国家
    x4 = []
    y4 = []
    i = 800
    while i < 1000:
        x4.append(newData[i][0])
        y4.append(newData[i][1])
        i += 1
    #四种颜色 红 略 蓝 黑
    plt.plot(x1, y1, "or")
    plt.plot(x2, y2, "og")
    plt.plot(x3, y3, "ob")
    plt.plot(x4, y4, "ok")
    plt.show()






















































