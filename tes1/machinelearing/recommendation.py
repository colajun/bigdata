#_*_  coding: utf-8 _*_
__author__ = "zhujun"
from math import  *



class recommendation:
    def __init__(self):
        self.critics = {"Lisa Rose": {"Lady in the Water": 2.5, "Snakes on a Plane": 3.5,
                                      "Superman Returns": 3.5, "You, Me and Dupree": 2.5},
                        "Gene Seymour": {"Lady in the Water": 3.0, "Snakes on a Plane":3.0,
                                         "Superman Returns":5.0, "The Night Listener":3.0},
                        "Michiael Phillips":{"Lady in the Water": 2.5, "Snakes on a Plane":3.0,
                                             "Superman Returns": 3.5, "The Night Listener": 4.0},
                        "Claudia Puig":{"Snakes on a Plane": 3.5, "Just My Luck":3.0,
                                        "Superman Returns": 3.0, "The Night Listener": 3.0},
                        "Mick LaSalle":{"Lady in the Water":3.0, "Snakes on a Plane":4.0,
                                        "Superman Returns": 3.0, "The Night Listener": 3.0},
                        "Jack Mattews":{"Lady in the Water": 3.0, "Snakes on a Plane":4.0,
                                        "Superman Retuens": 4.5, "You, Me and Dupree": 1.0},
                        "Toby":{"Snakes on a Plane": 4.5, "You, Me and Dupree":1.0,
                                "Superman Returns":4.0, "The Night Listerner":4.0}
                        }



    def transformPrefs(self):
        """
        将self.critics中的人--电影 的键和值进行调换，反向构造字典
        :return:
        """
        result = {}
        for person in self.critics:
            for item in self.critics[person]:
                result.setdefault(item, {})
                result[item][person]  = self.critics[person][item]
        return  result

    def sim_distance(self, person1, person2, new_critics=0):
        """
        使用欧式距离计算相似度，返回相似度是一个浮点数值，返回值为0代表没有相似度
        首先计算两个人所看相同电影评分的欧式具体L，如两个人没有观看相同电影则返回0，否则计算相似度公式
        其中的分母是L+1是为了防止分母为0的情况，欧式距离更大，相似度小。反之
        :param person1:
        :param person2:
        :param new_critics:
        :return:
        """
        if new_critics != 0:
            new_critics = self.transformPrefs()
            movielist1 = new_critics[person1]
            movielist2 = new_critics[person2]
            commonlist = []
            for item in movielist1:
                if item in movielist2:
                    commonlist.append(item)
            if len(commonlist) == 0:
                return 0
            distance = sum([pow(movielist1[item] - movielist2[item], 2) for item in commonlist])
            sim = 1 / (sqrt(distance) + 1)
            return  sim
        else:
            movielist1 = self.critics[person1]
            movielist2 = self.critics[person2]
            commonlist = []
            for item in movielist1:
                if item in movielist2:
                    commonlist.append(item)
            if len(commonlist) == 0:
                return 0
            distance = sum([pow(movielist1[item] - movielist2[item], 2) for item in commonlist])
            sim = 1 / (sqrt(distance) + 1)
            return  sim


    def sim_pearson(self, person1, person2):
        """
        计算皮尔孙相关系数，度量两个向量之间的线性相关性
        如果一个人总是给出比另一个人更高的分支， 但二者的分支只差基本保持一致，则他们仍然存在很好的相关性
        :param person1:
        :param person2:
        :return:
        """
        movielist1 = self.critics[person1]
        movielist2 = self.critics[person2]
        commonlist = []
        for item in movielist1:
            if item in movielist2:
                commonlist.append(item)

        if len(commonlist) == 0:
            return  0
        sum1 = sum([self.critics[person1][item] for item in commonlist])
        sum2 = sum([self.critics[person2][item] for item in commonlist])

        sum1sq = sum([pow(self.critics[person1][item], 2) for item in commonlist])
        sum2sq = sum([pow(self.critics[person2][item], 2) for item in commonlist])

        psum = sum([self.critics[person1][item] * self.critics[person2][item] for item in commonlist])

        n = len(commonlist)
        num = (psum -(sum1 * sum2 / n))
        den = sqrt((sum1sq - pow(sum1, 2) /n) * (sum2sq - pow(sum2, 2) / n))
        if den == 0:
            return  0
        r = num  / den
        return  r


    def topMatchers(self, person, n = 5, similarity=sim_pearson):
        """
        返回与person最相似的n个人的姓名和相似度，相似度度量选用皮尔孙相关系数
        :param person:
        :param n:
        :param similarity:
        :return:
        """
        if person not in self.critics:
            print "person inpur error!"
            return 0
        scores = [(similarity(self, person, item), item) for item in self.critics if item != person]
        return  sorted(scores, key=lambda  it: -it[0])[0:n]


    def getRecommendations(self, person, similarity=sim_pearson):
        """
        用与person的相似度对其他人对person未看过的影片进行加权平均，对person未看影片进行加权打分
        :param person:
        :param similarity:
        :return:
        """
        moviescore_dict =  {}
        mmoviesim_dict = {}
        for other in self.critics:
            sim = similarity(self, other, person)
            if other == person:
                continue
            if sim < 0:
                continue
            for item in self.critics[other]:
                if item not in self.critics[person]:
                    moviescore_dict.setdefault(item, 0)
                    moviescore_dict[item] += self.critics[other][item] * sim

                    mmoviesim_dict.setdefault(item, 0)
                    mmoviesim_dict[item] +=sim
        result = [(float(moviescore_dict[item] / mmoviesim_dict[item]), item) for item in moviescore_dict]
        result = sorted(result, key=lambda el: -el[0])

    def calculateSimilarItems(self, similarity=sim_distance):
        """
        利用反向构造的物品--人 字典，构造一个物品的相似度字典
        字典的建是物品， 值是与该物品最相似的n个物品以及物品之间的相似度
        :param similarity:
        :return:
        """
        result = {}
        reverse_critics = self.transformPrefs()
        for movie in reverse_critics:
            scoress = [(similarity(self, movie, item, 100) for item in reverse_critics if item != movie)]
            scores = sorted(scoress, key=lambda it: -it[0])
            result[movie] = scores
        return  result


    def getRecommendedItems(self, user):
        """
        指定一个名字user， 计算对该用户的推荐
        首先利用calculateSimilarItems方法计算物品之间 的相似度dict
        根据该用户对曾经看过的影片的评分以及该电影以未看过的电影的相似度估计用户对未看电影的评分
        :param user:
        :return:
        """
        userRatings =self.critics[user]
        itemMatch = self.calculateSimilarItems()
        scores = {}
        totalsim = {}
        for item, rating in userRatings.items():
            for similarity, item2 in itemMatch[item]:
                if item2 in userRatings:
                    continue
                scores.setdefault(item2, 0)
                scores[item2] ++ similarity * rating
                totalsim.setdefault(item2, 0)
                totalsim[item2] += similarity
        ranking = [(score / totalsim[item], item) for item, score in scores.items()]
        ranking = sorted(ranking, key=lambda it: -it[0])
        return  ranking


if __name__ == "__main__":
    system = recommendation()
    print "\n欧几里得相似度：", system.sim_distance("Lisa Rose", "Gene Seymour")


