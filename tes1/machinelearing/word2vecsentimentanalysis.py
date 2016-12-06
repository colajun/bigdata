#_*_ coding:utf-8_*_

from  gensim.models.word2vec import Word2Vec
from sklearn.cross_validation import train_test_split
import  jieba
import  sys
reload(sys)
import  numpy as np
from  sklearn.preprocessing import scale

def fenci(sentence):
    return " ".join(jieba.cut(sentence, cut_all=False))
# def getfencitext(filename):
#     with open(filename, "r") as infile:
#         review = infile.readlines()
#         segs= [jieba.cut(x, cut_all=False) for  x in review]
#         # stop = [line.strip().decode('utf-8') for line in open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/machinelearing/stopword.txt').readlines()]
#         # segs = jieba.cut('北京附近的租房skf轴承skf轴承西安总代理', cut_all=False)
#         # text = " ".join(list(set(segs) - set(stop)))
#         stop = set(u''':!),.:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
#         ﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
#         々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
#         ︽︿﹁﹃﹙﹛﹝（｛“‘-—_…''')
#         text = [" ".join(list(set(x) - stop)) for x in segs]
#         return  text


with open("/home/hadoop/bigdata/raw_pos.txt", "r") as infile:
    pos_weibo = infile.readlines()

with open("/home/hadoop/bigdata/raw_neg.txt", "r") as infile:
    neg_weibo = infile.readlines()
# pos_weibo = getfencitext("/home/hadoop/bigdata/raw_pos.txt")
# neg_weibo = getfencitext("/home/hadoop/bigdata/raw_neg.txt")
y =np.concatenate((np.ones(len(pos_weibo)), np.zeros(len(neg_weibo))))

x_train, x_test, y_train, y_test = train_test_split(np.concatenate((pos_weibo, neg_weibo)), y, test_size=0.2)


def cleanText(corpus):
    corpus = [fenci(z).replace("\n", "").split() for z in corpus]
    return  corpus

x_train = cleanText(x_train)
x_test = cleanText(x_test)
n_dim = 400

imdb_w2v = Word2Vec(size=n_dim, min_count=3)
imdb_w2v.build_vocab(x_train)
imdb_w2v.train(x_train)

def buildWordVector(text, size):
    vec = np.zeros(size).reshape((1, size))
    count =0
    for word in text:
        try:
            vec += imdb_w2v[word].reshape((1, size))
            count += 1
        except KeyError:
            continue
    if count != 0:
        vec /= count
    return  vec

train_vecs = np.concatenate([buildWordVector(z, n_dim) for z in x_train])
train_vecs =scale(train_vecs)
imdb_w2v.train(x_test)

test_vecs = np.concatenate([buildWordVector(z, n_dim) for z in x_test])
test_vecs = scale(test_vecs)


# from sklearn.linear_model import  SGDClassifier
#
# lr = SGDClassifier(loss="log", penalty="l1")
# lr.fit(train_vecs, y_train)
# print "Test Accuracy: %.2f" % lr.score(test_vecs, y_test)
# pred_probas = lr.predict_proba(test_vecs)[:,1]
# fpr, tpr,_ = roc_curve(y_test, pred_probas)
# roc_auc = auc(fpr, tpr)
# plt.plot(fpr, tpr, label="area=%.2f" % roc_auc)
#
# plt.plot([0, 1], [0, 1], "k--")
# plt.xlabel([0.0, 1.0])
# plt.ylim([0.0, 1.05])
# plt.legend(loc = "lower right")
# plt.show()

from  sklearn import  svm
lr = svm.SVC()
lr.fit(train_vecs, y_train)
print "Test Accuracy: %.2f" %lr.score(test_vecs, y_test)

from  sklearn.metrics import  roc_curve, auc
import  matplotlib.pyplot as plt















