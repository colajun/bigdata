#!/usr/bin/python

# -*- coding: utf-8 -*-

# coding=utf-8

import sys, urllib2, urllib

import MySQLdb as Mysql

import simplejson as json

import time, datetime

import socket

import warnings

import os

os.environ['TZ'] = 'Asia/Chongqing'

time.tzset()

warnings.filterwarnings("ignore", category=Mysql.Warning)


class IfengCommentCollector(object):
    """docstring for IfengCommentCollector"""

    # def __init__(self, arg):

    # 	super(IfengCommentCollector, self).__init__()

    # 	self.arg = arg

    def dbInit(self):

        # cfgFileObj = open('/home/appnews/DataBase.conf', 'r')
        cfgFileObj = open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/appnews/DataBase.conf', 'r')
        cfgContent = cfgFileObj.read()

        cfgFileObj.close()

        lineList = cfgContent.split('\n')

        hostName = lineList[0].split('=')[1]

        userName = lineList[1].split('=')[1]

        password = lineList[2].split('=')[1]

        selectDB = lineList[3].split('=')[1]

        self.con = Mysql.connect(host=hostName, user=userName, passwd=password, charset='utf8')

        print "enter dbInit "

        self.cursor = self.con.cursor()

        self.con.select_db(selectDB)

    def dbClose(self):

        print "enter dbclose"

        self.cursor.close()  # 释放游标

        self.con.commit()  # 提交数据

        self.con.close()  # 关闭数据库

    def updateComment(self, comment_table_name, news_id):

        content_table_name = comment_table_name.replace('comment', 'content')

        # print content_table_name

        sql = "select newsLink from " + content_table_name + " where news_id = '" + news_id + "'"

        query = self.cursor.execute(sql)

        # print query

        if query:
            row = self.cursor.fetchone()

            commentsUrl = row[0]

            self.GetNewsCommentByCommentsUrl(commentsUrl, news_id, comment_table_name)

    def GetNewsCommentByCommentsUrl(self, commentsUrl, docID, comment_table_name):  # 评论内容

        commentsPagenum = 1

        errorCounter = 0

        while True:

            url = "http://icomment.ifeng.com/geti.php?pagesize=20&p=" + str(
                commentsPagenum) + "&docurl=" + commentsUrl + "&type=all"

            # print url

            try:

                if errorCounter == 10:
                    break

                response = urllib2.urlopen(url)

                content = json.loads(response.read())

                response.close()

            except urllib2.HTTPError, e:

                errorCounter = errorCounter + 1

                print "Error Code:", e.code

            except urllib2.URLError, e:

                errorCounter = errorCounter + 1

                print "Error Reason:", e.reason

            except:

                errorCounter = errorCounter + 1

                print "Other Error"

            else:

                errorCounter = 0

                if not content["comments"]["newest"]:
                    # print "break"

                    break

                # print "commentsPagenum",commentsPagenum

                for eachcomment in content["comments"]["newest"]:

                    # commentTimeStamp = time.mktime(time.strptime(eachcomment["comment_date"], "%Y-%m-%d %H:%M"))

                    try:

                        commentTimeStamp = time.mktime(time.strptime(eachcomment["comment_date"], "%Y-%m-%d %H:%M"))

                    except:

                        commentTimeStamp = time.mktime(time.strptime(eachcomment["comment_date"], "%Y/%m/%d %H:%M"))

                    parentComment_content = ""

                    if eachcomment.has_key("parent"):  # 有引用评论

                        for parentComt in eachcomment["parent"]:
                            parentComment_content = parentComment_content + ";;;;;;" + parentComt["comment_contents"]

                    ifeng_comment_table_insert = "replace into " + comment_table_name + "(news_id, comment_id, user_from, agree_count, comment_time, comment_timestamp, content, parent_content) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"

                    self.cursor.execute(ifeng_comment_table_insert, (
                    docID, eachcomment["comment_id"], eachcomment["ip_from"], eachcomment["uptimes"],
                    eachcomment["comment_date"], int(commentTimeStamp), eachcomment["comment_contents"],
                    parentComment_content))

                    self.con.commit()

                commentsPagenum = commentsPagenum + 1


if __name__ == '__main__':

    if len(sys.argv) < 3:

        print 'No action specified.'

        sys.exit()

    else:

        print sys.argv[1], sys.argv[2]

    socket.setdefaulttimeout(20)

    ifeng = IfengCommentCollector()

    ifeng.dbInit()

    ifeng.updateComment(sys.argv[1], sys.argv[2])

    fetchSql = "update fetch_task set state = 2 where table_name = '" + sys.argv[1] + "' and news_id = '" + sys.argv[
        2] + "'"

    ifeng.cursor.execute(fetchSql)

    ifeng.dbClose()
