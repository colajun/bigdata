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



os.environ['TZ'] = 'Asia/Chongqing'

time.tzset()



warnings.filterwarnings("ignore", category = Mysql.Warning)



content_table_name = "zhihu_content_news_"+ time.strftime("%Y_%m")

comment_table_name = "zhihu_comment_news_"+ time.strftime("%Y_%m")

zhihu_comment_table_create = "CREATE TABLE if not exists " + comment_table_name + "\
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


zhihu_comment_table_insert = "replace into " + comment_table_name + "(mb_nick_name,char_name,user_name,news_id, comment_id, agree_count, comment_time, comment_timestamp, content, parent_content) VALUES(%s,%s,%s, %s, %s, %s, %s, %s, %s, %s)"



class IfengCommentCollector(object):


    def dbInit(self):
        # cfgFileObj = open('/home/appnews/DataBase.conf', 'r')
        cfgFileObj = open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/zhihumysqlself/DataBase.conf', 'r')

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

        self.cursor.execute(zhihu_comment_table_create)



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

			sql = "select news_id, commentNum, newsLink, Iner_use3 from " + content_table_name + " where publicTimestamp > " + str(daliyEndStp) + " and Iner_use4 = 1"

		elif updateMode == 'weekly':

			yesterday_stp = int(time.mktime(time.strptime(str(yesterday), "%Y-%m-%d")))

			lastWeek_stp = int(time.mktime(time.strptime(str(lastWeekDay), "%Y-%m-%d")))

			sql = "select news_id, commentNum, newsLink, Iner_use3 from " + content_table_name + " where publicTimestamp > " + str(lastWeek_stp) + " and publicTimestamp < " + str(yesterday_stp) + "  and Iner_use4 = 1"

		elif updateMode == "monthly":

			lastWeek_stp = int(time.mktime(time.strptime(str(lastWeekDay), "%Y-%m-%d")))

			lastMonth_stp = int(time.mktime(time.strptime(str(lastMonthDay), "%Y-%m-%d")))

			sql = "select news_id, commentNum, newsLink, Iner_use3 from " + content_table_name + " where publicTimestamp > " + str(lastMonth_stp) + " and publicTimestamp < " + str(lastWeek_stp) + " and Iner_use4 = 1"

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

		url = "http://news-at.zhihu.com/api/4/story-extra/"+documentId

		try:

			response = urllib2.urlopen(url)

			content = json.loads(response.read())

			response.close()

		except urllib2.HTTPError, e:

			# print "Error Code:", e.code

			return -1

		except urllib2.URLError, e:

			# print "Error Reason:", e.reason

			return -1

		except:

			pass

			return -1

		else:

			return int(content["comments"])



    def GetNewsCommentCountLong(self, documentId):

        url = "http://news-at.zhihu.com/api/4/story-extra/" + documentId

        try:

            response = urllib2.urlopen(url)

            content = json.loads(response.read())

            response.close()

        except urllib2.HTTPError, e:

            # print "Error Code:", e.code

            return -1

        except urllib2.URLError, e:

            # print "Error Reason:", e.reason

            return -1

        except:

            pass

            return -1

        else:

            return int(content["long_comments"])

    def GetNewsCommentCountShort(self, documentId):

        url = "http://news-at.zhihu.com/api/4/story-extra/" + documentId

        try:

            response = urllib2.urlopen(url)

            content = json.loads(response.read())

            response.close()

        except urllib2.HTTPError, e:

            # print "Error Code:", e.code

            return -1

        except urllib2.URLError, e:

            # print "Error Reason:", e.reason

            return -1

        except:

            pass

            return -1

        else:

            return int(content["short_comments"])

    def GetNewsCommentCountShortPopular(self, documentId):

        url = "http://news-at.zhihu.com/api/4/story-extra/" + documentId

        try:

            response = urllib2.urlopen(url)

            content = json.loads(response.read())

            response.close()

        except urllib2.HTTPError, e:

            # print "Error Code:", e.code

            return -1

        except urllib2.URLError, e:

            # print "Error Reason:", e.reason

            return -1

        except:

            pass

            return -1

        else:

            return int(content["popularity"])

    def GetNewsCommentByCommentsUrl(self, commentsUrl, docID, increment):   #评论内容
        url = "http://news-at.zhihu.com/api/4/story/"+docID+"/"
        if self.GetNewsCommentCountLong(docID) >0:
            url= url+"long-comments"
            try:

                response = urllib2.urlopen(url)

                content = json.loads(response.read())

                response.close()

            except urllib2.HTTPError, e:

                # print "Error Code:", e.code

                return -1

            except urllib2.URLError, e:

                # print "Error Reason:", e.reason

                return -1

            except:

                pass

                return -1

            else:
                for comment in content["comments"]:
                    author = comment["author"]
                    # advatar = comment["advator"]
                    content = comment["content"]
                    id = comment["id"]
                    # likes = comment["likes"]
                    # own = comment["own"]
                    timeStamp = comment["time"]
                    # voted = comment["voted"]
                    parent_content=""
                    parent_id = ""
                    if comment.has_key("reply_to"):
                        # parent_author = comment["reply_to"]["author"]
                        try:
                            parent_content = parent_content+",,,,,,"+comment["reply_to"]["content"]
                            parent_id = comment["reply_to"]["id"]
                        except:
                            parent_content = parent_content + ",,,,,," + comment["reply_to"]["error_msg"]
                    self.cursor.execute(zhihu_comment_table_insert, ("long",parent_id,author,
                    docID, id, self.GetNewsCommentCountShortPopular(docID),
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timeStamp)), int(timeStamp), content,
                    parent_content))
                    self.con.commit()






        elif self.GetNewsCommentCountShort(docID) >0:
            url= "http://news-at.zhihu.com/api/4/story/"+docID+"/"+"short-comments"
            try:

                response = urllib2.urlopen(url)

                content = json.loads(response.read())

                response.close()

            except urllib2.HTTPError, e:

                # print "Error Code:", e.code

                return -1

            except urllib2.URLError, e:

                # print "Error Reason:", e.reason

                return -1

            except:

                pass

                return -1

            else:
                for comment in content["comments"]:
                    author = comment["author"]
                    # advatar = comment["advator"]
                    content = comment["content"]
                    id = comment["id"]
                    # likes = comment["likes"]
                    # own = comment["own"]
                    timeStamp = comment["time"]
                    # voted = comment["voted"]
                    parent_content= ""
                    parent_id = ""
                    if comment.has_key("reply_to"):
                        # parent_author = comment["reply_to"]["author"]
                        try:
                            parent_content = parent_content+",,,,,,"+comment["reply_to"]["content"]
                            parent_id = comment["reply_to"]["id"]
                        except:
                            parent_content = parent_content + ",,,,,," + comment["reply_to"]["error_msg"]
                    self.cursor.execute(zhihu_comment_table_insert, ("short",parent_id,author,
                    docID, id, self.GetNewsCommentCountShortPopular(docID),
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timeStamp)), int(timeStamp), content,
                    parent_content))
                    self.con.commit()






if __name__ == '__main__':

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
