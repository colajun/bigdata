# -*- coding: utf8 -*-
import codecs
import os
import sys
from gensim.models.word2vec import Word2Vec
from gensim import models, corpora
from sklearn.svm import LinearSVC
import numpy as np
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
import logging
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.metrics import confusion_matrix
from sklearn.externals import joblib

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
import cPickle as pk
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import scale
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import RandomForestClassifier


def word2Vec(text, num_features, model):
    vec = np.zeros(num_features).reshape((1, num_features))
    num = 0
    for w in text:
        try:
            vec += model[w].reshape((1, num_features))
            num += 1.0
        except KeyError:
            continue
    if num != 0:
        vec /= num
    return vec


def temp(text, num_features, model):
    res = np.zeros(num_features)
    num = 0
    for w in text:
        if w in model.index2word:
            res += model[w]
            num += 1.0
    if num == 0:
        return np.zeros(num_features)
    else:
        return res / num


def getData():
    labels2id = {'neg': 1, 'pos': 2}
    train_corPth = u'/root/桌面/weibo/corpus/train'
    ans_corPth = u'/root/桌面/weibo/corpus/answer'
    train_data = []
    train_labels = []
    for category in os.listdir(train_corPth):
        wordSPth = train_corPth + '/' + category + '/' + 'corpus.wordStream'
        texts = codecs.open(wordSPth, 'r', 'utf-8').readlines()
        for text in texts:
            l = text.strip('\n').split()
            if len(l) > 4:
                train_data.append(l)
                train_labels.append(labels2id[category])

    test_data = []
    test_labels = []
    for category in os.listdir(ans_corPth):
        wordSPth = ans_corPth + '/' + category + '/' + 'corpus.wordStream'
        texts = codecs.open(wordSPth, 'r', 'utf-8').readlines()
        for text in texts:
            l = text.strip('\n').split()
            if len(l) > 4:
                test_data.append(l)
                test_labels.append(labels2id[category])
    return train_data, train_labels, test_data, test_labels


def train(train_data, train_labels, test_data, test_labels, size, min_count, window):
    data = []
    data.extend(train_data)
    data.extend(test_data)
    model = Word2Vec(data, size=size, min_count=min_count, window=window)
    # model = gensim.models.Word2Vec.load("wiki.zh.text.model")
    train_vecs = np.concatenate([word2Vec(text, size, model) for text in train_data])
    test_vecs = np.concatenate([word2Vec(text, size, model) for text in test_data])
    # clf = LinearSVC()
    # clf = GradientBoostingClassifier(n_estimators=5000)
    clf = RandomForestClassifier(n_estimators=1000)
    clf.fit(train_vecs, np.array(train_labels))
    pre_label = clf.predict(test_vecs)
    conf = confusion_matrix(np.array(test_labels), pre_label)
    accuracy = accuracy_score(np.array(test_labels), pre_label)
    precision = precision_score(np.array(test_labels), pre_label)
    recall = recall_score(np.array(test_labels), pre_label)
    f1 = f1_score(np.array(test_labels), pre_label)
    return accuracy, precision, recall, f1, conf


import gensim


def train1(train_data, train_labels, test_data, test_labels, size, min_count, window):
    data = []
    data.extend(train_data)
    data.extend(test_data)
    model = Word2Vec(data, size=size, min_count=min_count, window=window)
    # model = gensim.models.Word2Vec.load("wiki.zh.text.model")
    train_vecs = np.zeros([len(train_data), size], float)
    for i, sent in enumerate(train_data):
        train_vecs[i, :] = temp(sent, size, model)
    test_vecs = np.zeros([len(test_data), size], float)
    for i, sent in enumerate(test_data):
        test_vecs[i, :] = temp(sent, size, model)
    clf = RandomForestClassifier(n_estimators=1000)
    clf.fit(train_vecs, np.array(train_labels))
    pre_label = clf.predict(test_vecs)
    accuracy = accuracy_score(np.array(test_labels), pre_label)
    precision = precision_score(np.array(test_labels), pre_label)
    recall = recall_score(np.array(test_labels), pre_label)
    f1 = f1_score(np.array(test_labels), pre_label)
    return accuracy, precision, recall, f1


if __name__ == '__main__':
    import time

    t1 = time.time()
    import codecs

    train_data, train_labels, test_data, test_labels = getData()
    accuracy, precision, recall, f1, conf = train(train_data, train_labels, test_data, test_labels, 400, 5, 50)
    print accuracy, precision, recall, f1
    # accuracy,precision,recall,f1 = train1(train_data, train_labels, test_data, test_labels, 200, 5, 50)
    # print accuracy,precision,recall,f1
    t2 = time.time()
    print t2 - t1
