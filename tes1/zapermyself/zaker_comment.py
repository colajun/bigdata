#!/usr/bin/python
# -*- coding: utf-8 -*-
#coding=utf-8

import sys,urllib2,urllib

import MySQLdb as Mysql

import simplejson as json

import time,datetime

import socket

import warnings

import os

import  gzip
import  StringIO



os.environ['TZ'] = 'Asia/Chongqing'

time.tzset()



warnings.filterwarnings("ignore", category = Mysql.Warning)



content_table_name = "zaker_content_news_"+ time.strftime("%Y_%m")

comment_table_name = "zaker_comment_news_"+ time.strftime("%Y_%m")

zaker_comment_table_create = "CREATE TABLE if not exists " + comment_table_name + "\
						  (`id` int(11) NOT NULL AUTO_INCREMENT,\
						  `news_id` varchar(60) DEFAULT NULL,\
						  `comment_id` varchar(50) DEFAULT NULL,\
						  `user_name` varchar(50) DEFAULT NULL,\
						  `user_from` varchar(50) DEFAULT NULL,\
						  `agree_count` int(11) DEFAULT NULL,\
						  `head_url` text,\
						  `comment_time` varchar(20) DEFAULT NULL,\
						  `comment_timestamp` int(11) DEFAULT NULL,\
						  `content` text,\
						  `parent_content` text,\
						  `mb_nick_name` varchar(50) DEFAULT NULL,\
						  `char_name` varchar(50) DEFAULT NULL,\
						  `mb_head_url` text,\
						  `mb_isgroupvip` text,\
						  `mb_isvip` text,\
						  `sex` text,\
						  `uin` text,\
                         UNIQUE INDEX `comment_id` (`comment_id`, `news_id`),\
						  PRIMARY KEY (`id`)\
							)COLLATE='utf8_general_ci'"


zaker_comment_table_insert = "replace into " + comment_table_name + "(mb_nick_name, char_name,news_id, comment_id, user_name, user_from, agree_count, comment_time, comment_timestamp, content, parent_content) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"



class IfengCommentCollector(object):

    def __init__(self):
        self.headers = {
            "Connection": "Keep-Alive",
            "Host": "hotphone.myzaker.com",
            # "User-Agent": "Dalvik/1.6.0 (Linux; U; Android 4.2.2; Google Nexus 4 - 4.2.2 - API 17 - 768x1280 Build/JDQ39E)"
            "User-Agent": "Mozilla/5.0 (Linux; U; Android 4.4.4; zh-cn;Custom Phone - 4.4.4 - API 19 - 768x1280 Build/JOP40D; CyanogenMod-10.1) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30"

        }

    def dbInit(self):
        # cfgFileObj = open('/home/appnews/DataBase.conf', 'r')
        cfgFileObj = open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/zapermyself/DataBase.conf', 'r')

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

        self.cursor.execute("create database if not exists " + selectDB)

        self.con.select_db(selectDB)

        self.cursor.execute(zaker_comment_table_create)



    def dbClose(self):

		print "enter dbclose"

		self.cursor.close()    #释放游标

		self.con.commit()        #提交数据

		self.con.close()        #关闭数据库



    def updateComment(self, updateMode):

		today = datetime.date.today()

		yesterday = today + datetime.timedelta(days = -1)

		lastWeekDay = today + datetime.timedelta(days = -7)

		lastMonthDay = today + datetime.timedelta(days= -30)

		if updateMode == 'daily':

			daliyEndStp = int(time.mktime(time.strptime(str(today), "%Y-%m-%d")))

			# print today, daliyEndStp

			sql = "select news_id, commentNum, newsLink, Iner_use3 from " + content_table_name + " where  " + "  Iner_use4 = 1"

		elif updateMode == 'weekly':

			yesterday_stp = int(time.mktime(time.strptime(str(yesterday), "%Y-%m-%d")))

			lastWeek_stp = int(time.mktime(time.strptime(str(lastWeekDay), "%Y-%m-%d")))

			sql = "select news_id, commentNum, newsLink, Iner_use3 from " + content_table_name + " where "  + "  Iner_use4 = 1"

		elif updateMode == "monthly":

			lastWeek_stp = int(time.mktime(time.strptime(str(lastWeekDay), "%Y-%m-%d")))

			lastMonth_stp = int(time.mktime(time.strptime(str(lastMonthDay), "%Y-%m-%d")))

			sql = "select news_id, commentNum, newsLink, Iner_use3 from " + content_table_name + " where  " +"  Iner_use4 = 1"

		else:

			sys.exit()

		query = self.cursor.execute(sql)
		print query

		if query:

			rows = self.cursor.fetchall()

			for row in rows:

				documentId = row[0]

				commentsUrl = row[2]

				Iner_use3 = row[3]

				commentsAll = 0

				newlycommentNum = self.GetNewsCommentCount(documentId)


				if newlycommentNum == -1:

					continue

				if Iner_use3 == 0:

					commentsAll = 0

					self.cursor.execute("update " + content_table_name + " set Iner_use3 = 1 where news_id = '" + documentId + "'")

					self.con.commit()

				else:

					commentsAll = row[1]

				if newlycommentNum > commentsAll:

					updateSql = "update " + content_table_name + " set commentNum = " + str(newlycommentNum) + " where news_id = '" + documentId + "'"

					self.cursor.execute(updateSql)

					self.con.commit()

					increment = newlycommentNum - commentsAll

					self.GetNewsCommentByCommentsUrl(commentsUrl, documentId, increment)






    def GetNewsCommentCount(self, documentId):

        url = "http://c.myzaker.com/weibo/api_comment_article_url.php?_appid=AndroidPhone&_dev=28&_v=7.0.2&_version=7.0.2&act=get_comments&pk=" + str(
            documentId)
        try:
            req = urllib2.Request(url)
            req.add_header('User-agent', self.headers.get("User-agent"))
            response = urllib2.urlopen(req)
            content = json.loads(response.read())

        except urllib2.HTTPError, e:

            print "Error Code:", e.code

        except urllib2.URLError, e:

            print "Error Reason:", e.reason
        except:
            print "Other Error:"
        else:
            return int(content["data"]["comment_counts"])

        # url = "http://c.myzaker.com/weibo/api_comment_article_url.php?_appid=AndroidPhone&_dev=28&_v=7.0.2&_version=7.0.2&act=get_comments&pk="+ str(documentId)
        # try:
        #     req = urllib2.Request(url)
        #     req.add_header('User-agent', ifeng.headers.get("User-agent"))
        #     response = urllib2.urlopen(req)
        #     content = json.loads(response.read())
        #
        # except urllib2.HTTPError, e:
        #
        #     print "Error Code:", e.code
        #
        # except urllib2.URLError, e:
        #
        #     print "Error Reason:", e.reason
        # except:
        #     print "Other Error:"
        # else:
        #     return int(content["data"]["comment_counts"])




    def GetNewsCommentByCommentsUrl(self, commentsUrl, docID, increment):   #评论内容

		# print "increment", increment



        url = "http://c.myzaker.com/weibo/api_comment_article_url.php?_appid=AndroidPhone&_dev=28&_v=7.0.2&_version=7.0.2&act=get_comments&pk=" + docID  # print url
        while(url != ""):
            try:
                req = urllib2.Request(url)
                req.add_header('User-agent', self.headers.get("User-agent"))
                response = urllib2.urlopen(req)
                content = json.loads(response.read())

            except urllib2.HTTPError, e:

                print "Error Code:", e.code

            except urllib2.URLError, e:

                print "Error Reason:", e.reason
            except:
                print "Other Error:"
            else:
                parentComment_content = ""
                parent_pk = ""
                for comment in content["data"]["comments"][0]["list"]:
                    author_name = comment["auther_name"]
                    contents = comment["content"]
                    date = comment["date"] +" "+ comment["time"]
                    like_num = comment["like_num"]
                    pk = comment["pk"]
                    title = content["data"]["comments"][0]["title"]
                    if comment.has_key("reply_comment"):
                        parentComment_content = ";;;;;;;;;;" + comment["reply_comment"]["content"]
                        parent_pk = comment["reply_comment"]["pk"]
                        # "(mb_nick_name, char_name,news_id, comment_id, user_from, agree_count, comment_time, comment_timestamp, content, parent_content) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    self.cursor.execute(zaker_comment_table_insert, (title,parent_pk,
                                                                     docID, pk, author_name, "", like_num, date,
                                                                     int(time.mktime(
                                                                         time.strptime(date, "%Y-%m-%d %H:%M:%S"))),
                                                                     contents, parentComment_content))
                    self.con.commit()
                try:
                    url = content["data"]["comments"]["next_url"]
                except:
                    url=""
                    pass








if __name__ == '__main__':
    # ifeng = IfengCommentCollector()
    # url = "http://c.myzaker.com/weibo/api_comment_article_url.php?_appid=AndroidPhone&_dev=28&_v=7.0.2&_version=7.0.2&act=get_comments&pk=" + "582a622e9490cbea57000012"  # print url
    # try:
    #     req = urllib2.Request(url)
    #     req.add_header('User-agent', ifeng.headers.get("User-agent"))
    #     response = urllib2.urlopen(req)
    #     content = json.loads(response.read())
    #
    # except urllib2.HTTPError, e:
    #
    #     print "Error Code:", e.code
    #
    # except urllib2.URLError, e:
    #
    #     print "Error Reason:", e.reason
    # except:
    #     print "Other Error:"
    # else:
    #     for comment in content["data"]["comments"][0]["list"]:
    #         author_name = comment["auther_name"]
    #         contents = comment["content"]
    #         date = comment["date"] + comment["time"]
    #         like_num = comment["like_num"]
    #         pk = comment["pk"]
    #         title = content["data"]["comments"][0]["title"]
    #         print  title
#
	if len(sys.argv) < 2:

		print 'No action specified.'

		sys.exit()

	else:

		print sys.argv[1]

	socket.setdefaulttimeout(20)

	ifeng = IfengCommentCollector()

	ifeng.dbInit()

	ifeng.updateComment(sys.argv[1])

	ifeng.dbClose()






