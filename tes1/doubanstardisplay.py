#!/usr/bin/python
#_*_ coding:UTF-8 _*_

import  MySQLdb
import  numpy as np
import  matplotlib.pylab as plt

star1 = 0
star2 = 0
star3 = 0
star4 = 0
star5 = 0
db = MySQLdb.Connect("localhost", "root", "root", "crawler")
cursor = db.cursor()
cursor.execute("select  startcount from tb_doubancomments2")
for row in cursor.fetchall():
    if row == (1L,):
        star1 += 1
    if row == (2L,):
        star2 += 1
    if row == (3L,):
        star3 += 1
    if row == (4L,):
        star4 += 1
    if row == (5L,):
        star5 += 1
cursor.close()


# n_groups = 1
#
# one = (star1)
# two = (star2)
# three = (star3)
# four = (star4)
# five = (star5)
# fig, ax = plt.subplots()
# index = np.arange(n_groups)
# bar_width = 0.35
#
# opacity = 0.4
# rects1 = plt.bar(index, one, bar_width, alpha=opacity, color="b")
# rects2 = plt.bar(index+bar_width, two, bar_width, alpha=opacity, color="r")
# rects3 = plt.bar(index+bar_width, three, bar_width, alpha=opacity, color="g")
# rects4 = plt.bar(index+bar_width, four, bar_width, alpha=opacity, color="b")
# rects4 = plt.bar(index+bar_width, five, bar_width, alpha=opacity, color="b")
#
# plt.xlabel("star")
# plt.ylabel("startcount")
# plt.title("疯狂动物城的各星级评论条数")
# plt.xticks(index + bar_width/2, ("star1", "star2", "star3", "star4", "star5"))
# plt.ylim(0, 100000)
# plt.tight_layout()
# plt.show()


























