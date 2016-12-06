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



content_table_name = "rmr_content_news_"+ time.strftime("%Y_%m")

comment_table_name = "rmr_comment_news_"+ time.strftime("%Y_%m")

rmr_comment_table_create = "CREATE TABLE if not exists " + comment_table_name + "\
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


rmr_comment_table_insert = "replace into " + comment_table_name + "(news_id, comment_id, user_from, agree_count, comment_time, comment_timestamp, content, parent_content) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"



class ParseConf(object):
    """
    Parse Conf File
    """

    def __init__(self):

        # conf_file = "/home/appnews/ifeng/news360.conf"
        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/remingmyself/rmrcomment.conf"
        if not os.path.exists(conf_file):
            print "Not Found Parse Conf File!"
            return None
        else:
            self.conf_file = conf_file
            self.parse()

    def parse(self):

        fileHandle = open(self.conf_file)
        data = fileHandle.read()
        fileHandle.close()
        # data = data.replace("\r","").replace("\n","").replace(" ","").replace("\t","")
        # print data
        data = json.loads(data)

        # self.rmr_list_url = data["rmr_list_url"]
        # self.cj_list_url = data["cj_list_url"]
        self.news_list_param = data["news_list_param"]


class IfengCommentCollector(object):

    def __init__(self):

        parse_api = ParseConf()
        # self.rmr_list_url = parse_api.rmr_list_url
        # self.cj_list_url = parse_api.cj_list_url
        self.news_list_param = parse_api.news_list_param
        self.querystring = urllib.urlencode(self.news_list_param)


    def dbInit(self):
        # cfgFileObj = open('/home/appnews/DataBase.conf', 'r')
        cfgFileObj = open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/remingmyself/DataBase.conf', 'r')

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

        self.cursor.execute(rmr_comment_table_create)



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

			sql = "select news_id, commentNum, Iner_use6, Iner_use3 from " + content_table_name + " where publicTimestamp > " + str(daliyEndStp)+" and Iner_use4 =1"

		elif updateMode == 'weekly':

			yesterday_stp = int(time.mktime(time.strptime(str(yesterday), "%Y-%m-%d")))

			lastWeek_stp = int(time.mktime(time.strptime(str(lastWeekDay), "%Y-%m-%d")))

			sql = "select news_id, commentNum, Iner_use6, Iner_use3 from " + content_table_name + " where publicTimestamp > " + str(lastWeek_stp) + " and publicTimestamp < " + str(yesterday_stp)+" and Iner_use4 =1"

		elif updateMode == "monthly":

			lastWeek_stp = int(time.mktime(time.strptime(str(lastWeekDay), "%Y-%m-%d")))

			lastMonth_stp = int(time.mktime(time.strptime(str(lastMonthDay), "%Y-%m-%d")))
			sql = "select news_id, commentNum, Iner_use6, Iner_use3,id from " + content_table_name + " where publicTimestamp > " + str(lastMonth_stp) + " and publicTimestamp < " + str(lastWeek_stp)+" and Iner_use4 =1"

		else:

			sys.exit()

		query = self.cursor.execute(sql)
		print query

		if query:

			rows = self.cursor.fetchall()

			for row in rows:

				documentId = row[0]

				commentsUrl = "http://rmrbapi.people.cn/comments/commentlist?count=20&articleid="+documentId+"&categoryi"

				Iner_use3 = row[3]

				commentsAll = 0

				newlycommentNum = self.GetNewsCommentCount(commentsUrl)

				if (newlycommentNum == -1):

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

					# increment = newlycommentNum - commentsAll

					self.GetNewsCommentByCommentsUrl(commentsUrl, documentId, newlycommentNum)

    def GetNewsCommentCount(self, commentsUrl):

        url = commentsUrl+self.querystring

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
            if content["data"].has_key("count"):
                return content["data"]["count"]
            else:
                return  -1



    def GetNewsCommentByCommentsUrl(self, commentsUrl, docID, newlycommentNum):
        # print "increment", increment


        commentCounter = 0

        errorCounter = 0
        # go = True
        url = "http://rmrbapi.people.cn/comments/getdetailcomments?count=" + str(
            newlycommentNum) + "&articleid=" + docID + "&" + self.querystring

        # print url

        try:

            if errorCounter == 10:
                return

            response = urllib2.urlopen(url)

            content = json.loads(response.read())

            response.close()

        except KeyError, e:

            errorCounter = errorCounter + 1

        # print "Error Code:", e.code

        except urllib2.URLError, e:

            errorCounter = errorCounter + 1

        # print "Error Reason:", e.reason

        except:

            errorCounter = errorCounter + 1

        # print "Other Error"

        else:
            try:
                for eachcomment in content["data"]["comments"]:

                    commentCounter = commentCounter + 1

                    area = eachcomment["user"]["user_area"]
                    # articleid = eachcomment["article_id"]
                    timestamp = eachcomment["timestamp"]
                    comment_id = eachcomment["comment_id"]
                    like_num = eachcomment["like_num"]
                    time = eachcomment["time"]
                    content = eachcomment["content"]
                    # user_name = eachcomment["user"]["user_name"]

                    parentComment_content = ""
                    if eachcomment.has_key("replay_comment"):  # 有引用评论

                        for parentComt in eachcomment["replay_comment"]:
                            # area_replay = parentComt["user"]["user_area"]
                            # articleid_replay = parentComt["article_id"]
                            # timestamp_replay = parentComt["timestamp"]
                            # comment_id_replay = parentComt["comment_id"]
                            # like_num_replay = parentComt["like_num"]
                            # time_replay = parentComt["time"]
                            content_replay = parentComt["content"]
                            # user_name_replay = parentComt["user"]["user_name"]
                            parentComment_content = parentComment_content + ";;;;;;" + content_replay

                    self.cursor.execute(rmr_comment_table_insert, (
                        docID, comment_id,
                        area, like_num, time, timestamp, content,
                        parentComment_content))

                    self.con.commit()
            except TypeError,e:
                pass
        # go = False







if __name__ == '__main__':

    if len(sys.argv) < 2:

    	print 'No action specified.'

    	sys.exit()

    else:

    	print sys.argv[1]

    socket.setdefaulttimeout(20)
    #
    rmr = IfengCommentCollector()
    # print rmr.GetNewsCommentCount("http://rmrbapi.people.cn/content/getdetail?articleid=1916582953927680_cms_1916582953927680&categoryi"+rmr.querystring)
    #
    rmr.dbInit()

    rmr.updateComment(sys.argv[1])

    rmr.dbClose()
    # url = "http://rmrbapi.people.cn/comments/getdetailcomments?count=387&articleid=2998_cms_1915725146686464&" + rmr.querystring
    # url = "http://rmrbapi.people.cn/comments/commentlist?count=2000&articleid=1916582953927680_cms_1916582953927680&categoryi&" + rmr.querystring
    # response = urllib2.urlopen(url)
    # content = json.loads(response.read())
    # if content["data"].has_key("count"):
    #     print "*********"
    #     print int(content["data"]["count"])
    # try:
    #     commentNum = int(content["data"]["count"])
    # except KeyError, e:
    #     commentNum = 2222
    # except TypeError, e:
    #     commentNum = 2222
    #
    # if commentNum == 2222:
    #     commentNum = 300000
    # print commentNum
    # counter =0
    # print int(content["data"]["count"])
    # for eachcomment in content["data"]["comments"]:
    #     area = eachcomment["user"]["user_area"]
    #     articleid = eachcomment["article_id"]
    #     timestamp = eachcomment["timestamp"]
    #     comment_id = eachcomment["comment_id"]
    #     like_num = eachcomment["like_num"]
    #     time = eachcomment["time"]
    #     content = eachcomment["content"]
    #     user_name = eachcomment["user"]["user_name"]
    #
    #     if eachcomment.has_key("replay_comment"):  # 有引用评论
    #
    #         for parentComt in eachcomment["replay_comment"]:
    #             area = parentComt["user"]["user_area"]
    #             articleid = parentComt["article_id"]
    #             timestamp = parentComt["timestamp"]
    #             comment_id = parentComt["comment_id"]
    #             like_num = parentComt["like_num"]
    #             time = parentComt["time"]
    #             content = parentComt["content"]
    #             user_name = parentComt["user"]["user_name"]
    #             print user_name
    #     counter += 1
    #     print  counter
    # response.close()



