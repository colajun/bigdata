#_*_coding:utf-8_*_

from gensim.models.word2vec import  Word2Vec
import  jieba
import numpy as np
from  sklearn.cross_validation import train_test_split
from sklearn.preprocessing import  scale
from  sklearn import  svm
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import  roc_curve, auc
import matplotlib.pyplot as plt



n_dim = 400

def getstopwordset(stopfile):
    return  set([line.strip().decode("utf-8") for line in open(stopfile)])

def getfencitext(filename):
    with open(filename, "r") as infile:
        review = infile.readlines()
        segs= [jieba.cut(x, cut_all=False) for  x in review]
        # stop = [line.strip().decode('utf-8') for line in open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/machinelearing/stopword.txt').readlines()]
        # segs = jieba.cut('北京附近的租房skf轴承skf轴承西安总代理', cut_all=False)
        # text = " ".join(list(set(segs) - set(stop)))
        stop = set(u''':!),.:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
        ﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
        々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
        ︽︿﹁﹃﹙﹛﹝（｛“‘-—_… 的 了 蒙 牛 是 我 在 / 不 也 @ 有 和 就 很 还 http 都 t # cn 1 这 2'''.split(" "))
        text = [" ".join(list(set(x) - stop)) for x in segs]
        return  text


# res14: Array[(String, Int)] = Array((的,3987), (了,2807), (蒙牛,2060), (是,2039), (我,1739), (在,1737), (/,1499), (不,1489), (也,1371), (@,1238), (有,1143), (和,1123), (就,1103), (很,994), (还,956), (http,910), (都,889), (t,871), (#,864), (cn,830), (1,689), (这,675), (2,674))


def getfencitextwrite(filename, writefile):
    with open(filename, "r") as infile:
        review = infile.readlines()
        segs= [jieba.cut(x, cut_all=False) for  x in review]
        # stop = [line.strip().decode('utf-8') for line in open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/machinelearing/stopword.txt').readlines()]
        # segs = jieba.cut('北京附近的租房skf轴承skf轴承西安总代理', cut_all=False)
        # text = " ".join(list(set(segs) - set(stop)))
        stop = set(u''':!),.:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
        ﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
        々‖•·ˇˉ―--′’”([{£¥'"‵‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
        ︽︿﹁﹃﹙﹛﹝（｛“‘-—_…''')
        text = [" ".join(list(set(x) - stop)) for x in segs]
        with open(writefile, "w") as writefiles:
            [writefiles.write(x.encode("utf-8")+"\n") for x in text]







def getsplitdata(posfile, negfile):
    pos_reviews = getfencitext(posfile)
    neg_reviews = getfencitext(negfile)
    y = np.concatenate((np.ones(len(pos_reviews)), np.zeros(len(neg_reviews))))
    x_train, x_test, y_train, y_test = train_test_split(np.concatenate((pos_reviews, neg_reviews)), y, test_size=0.2)
    return  x_train, x_test, y_train, y_test

def getvev(x_trainandtest, imdb_w2v):
    vec = np.zeros(n_dim).reshape(1, n_dim)
    count =0.
    for word in x_trainandtest:
        try:
            vec += imdb_w2v[word].reshape((1, n_dim))
            count += 1
        except KeyError:
            continue
    if count != 0:
        vec /= count
    return  vec


def getendvec(dataset):
    train_vecs = np.concatenate([getvev(z, imdb_w2v) for z in dataset])
    train_vecs = scale(train_vecs)
    return  train_vecs

def graphroc(model,test_vecs, y_test):
    pred_probas = model.predict_proba(test_vecs)[:, 1]
    fpr, tpr, _ =roc_curve(y_test, pred_probas)
    roc_auc = auc(fpr, tpr)
    plt.plot(fpr, tpr, label="area=%.2f" % roc_auc)
    plt.plot([0, 1], [0, 1], "k--")
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.legend(loc="lower right")
    plt.show()



if __name__ =="__main__":
    x_train, x_test, y_train, y_test= getsplitdata("/home/hadoop/bigdata/raw_pos.txt", "/home/hadoop/bigdata/raw_neg.txt")
    # for x in x_train:
    #     print x
    imdb_w2v = Word2Vec(size=n_dim, min_count=4, window=50)
    imdb_w2v.build_vocab(x_train)
    imdb_w2v.train(x_train)

    x_train_avg = getendvec(x_train)

   # imdb_w2v.train(x_test)

    x_test_avg = getendvec(x_test)
    clf = svm.SVC()
    clf.fit(x_train_avg, y_train)
    # clf = RandomForestClassifier(n_estimators=1000)
    # clf.fit(x_train_avg, y_train)


    print "Test Accuracy: %.2f" % clf.score(x_test_avg, y_test)
    # graphroc(clf,x_test_avg, y_test)
   # #  print getstopwordset("/home/hadoop/bigdata/workspacepython/bigdata/tes1/machinelearing/stopword.txt")
   #  getfencitextwrite("/home/hadoop/bigdata/raw_pos.txt","/home/hadoop/bigdata/workspacepython/bigdata/tes1/machinelearing/pos.txt")
   #  getfencitextwrite("/home/hadoop/bigdata/raw_neg.txt","/home/hadoop/bigdata/workspacepython/bigdata/tes1/machinelearing/neg.txt")