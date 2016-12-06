#_*_coding:utf-8_*_

#%%朴素贝野史(针对离散输入变量)
import  numpy as np

class NaiBayes(object):
    def __init__(self,train_x, train_y):
        self.train_x = train_x
        self.train_y = train_y
        self.dimension = len(train_x[0])
        self.n_sample = len(self.train_y)
        self.labels = np.unique(self.train_y)
        #计算label的鲜艳概念和feature各纬度的条件概率
        self.pre_prob = self.cal_pre_prob()
        self.condi_prob = self.cal_condi_prob()

    #计算y的鲜艳概率
    def cal_pre_prob(self):
        pre_prob = {}
        for y in self.labels:
            pre_prob[y] = self.train_y.count(y)/float(len(self.train_y))
        return pre_prob

    #计算各特征纬度的条件概率
    def cal_condi_prob(self):
        condi_prob = {}
        dim_x = zip(*self.train_x)
        for i, xi in enumerate(dim_x):
            xi = np.array(xi)
            for xij in np.unique(xi):
                bool_xij = xi ==xij
                for y in self.labels:
                    #p(xij|y)第i各纬度取值为xij的特质
                    bool_y = self.train_y == y
                    condi_prob[(i, xij, y)] = sum(bool_y&bool_xij)/float(sum(bool_y))
        return  condi_prob

    def predict(self, x):
        if len(x) != self.dimension:
            raise "feature dimension not equal!"
        prob = {}
        for y in self.labels:
            prob[y] = self.pre_prob[y]
            for i,xi in enumerate(x):
                prob[y] *= self.condi_prob[(i,xi,y)]
        #计算出标签概率最大的那个
        print prob
        prob_sum = sum(prob.values())
        max_label, max_prob = None, 0
        for la in prob.keys():
            if prob[la] > max_prob:
                max_prob = prob[la]
                max_label = la
        return  max_label, max_prob/float(prob_sum)


def test_NaiveBayes():
    x=[[1, "s"], [1, "m"], ['1', "m"], [1, "s"], [1, "s"], [2, "s"], [2, "m"], [2, "m"],
    [2, "1"],[2, "1"],[3, "1"], [3, "m"], [3, "m"], [3, "1"], [3, "1"]]
    y = [-1,-1,1,1,-1,-1,-1]+[1]*7 + [-1]
    cls = NaiBayes(x, y)
    new_x = [2, "s"]
    print cls.predict(new_x)


test_NaiveBayes()






















