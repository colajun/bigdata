#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8
import sys, urllib2, urllib
import MySQLdb as Mysql
import simplejson as json
import time
import socket
import warnings
import os

warnings.filterwarnings("ignore", category=Mysql.Warning)
os.environ['TZ'] = 'Asia/Chongqing'
time.tzset()

content_table_name = "haoqixin_content_news_" + time.strftime("%Y_%m")
haoqixin_news_table_create = "CREATE TABLE if not exists " + content_table_name + "(\
						  `id` int(11) NOT NULL AUTO_INCREMENT,\
						  `news_id` varchar(40) DEFAULT NULL,\
						  `channelName` varchar(20) DEFAULT NULL,\
						  `channelId` varchar(20) DEFAULT NULL,\
						  `title` text,\
						  `newsFrom` varchar(50) DEFAULT NULL,\
						  `newsLink` text,\
						  `publicTime` varchar(20) DEFAULT NULL,\
						  `publicTimestamp` int(11) DEFAULT NULL,\
						  `abstract` text,\
						  `content` text,\
						  `commentNum` int(11) DEFAULT 0,\
						  `Iner_use1` varchar(100) DEFAULT NULL,\
						  `Iner_use2` varchar(100) DEFAULT NULL,\
						  `Iner_use3` int(1) DEFAULT 0,\
						  `Iner_use4` int(1) DEFAULT 0,\
  						  `Iner_use5` varchar(100) DEFAULT NULL,\
						  `Iner_use6` varchar(100) DEFAULT NULL,\
						  PRIMARY KEY (`id`),\
						  UNIQUE INDEX `news_id` (`news_id`, `channelId`),\
						  KEY `channelId` (`channelId`),\
						  KEY `commentNum` (`commentNum`),\
						  KEY `Iner_use1` (`Iner_use1`),\
						  KEY `Iner_use2` (`Iner_use2`)\
						)COLLATE='utf8_general_ci'"


class ParseConf(object):
    """
    Parse Conf File
    """

    def __init__(self):

        # conf_file = "/home/appnews/ifeng/news360.conf"
        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/haoqixinmyself/haoqixin.conf"
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

        self.haoqixin_list_url = data["haoqixin_list_url"]
        # self.cj_list_url = data["cj_list_url"]
        self.news_list_param = data["news_list_param"]


class NewsListCollector(object):
    """docstring for NewsListCollector"""

    # def __init__(self, arg):
    # 	super(NewsListCollector, self).__init__()
    # 	self.arg = arg

    def __init__(self):

        parse_api = ParseConf()
        self.haoqixin_list_url = parse_api.haoqixin_list_url
        # self.cj_list_url = parse_api.cj_list_url
        self.news_list_param = parse_api.news_list_param
        self.channel_info_dict = {}

        self.headers = {
            "Connection": "Keep-Alive",
            "Host": "api.iclient.ifeng.com",
            "User-Agent": "Dalvik/1.6.0 (Linux; U; Android 4.2.2; Google Nexus 4 - 4.2.2 - API 17 - 768x1280 Build/JDQ39E)"
        }

    def dbInit(self):
        # cfgFileObj = open('/home/appnews/DataBase.conf', 'r')
        cfgFileObj = open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/haoqixinmyself/DataBase.conf', 'r')
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
        self.cursor.execute(haoqixin_news_table_create)

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def Starter(self):
        self.cursor.execute("select channelId , channelName from channellist where appName = 'haoqixin' and recommend > 0")
        rows = self.cursor.fetchall()
        for row in rows:
            channelId = row[0]
            channelName = row[1]
            print "channelId ===================", channelId
            self.GetNewsList(channelId, channelName)
    def GetNewsList(self, channelId, channelName):
        url = ""
        if channelId == 'index':
            for counter in [0, 38, 47, 15, 31, 44,33]:
                if(counter ==0):
                    url = "http://app3.qdaily.com/app3/homes/index/" + str(counter) + ".json"
                if(counter !=0):
                    url = "http://app3.qdaily.com/app3/columns/index/" + str(counter) + "/0.json"
                try:
                    response = urllib2.urlopen(url)
                    content = json.loads(response.read())["response"]
                    response.close()
                except urllib2.HTTPError, e:
                    print "Error Code:", e.code
                except urllib2.URLError, e:
                    print "Error Reason:", e.reason
                    # except:
                    #     # print "Other Error"
                    #     # pass
                else:
                    if content.has_key("banners"):
                        for feed in content["banners"]:
                            news_id = feed["post"]["id"]
                            title = feed["post"]["title"]
                            # news_From = feed[""]
                            commentNum=feed["post"]["comment_count"]
                            abstract = feed["post"]["description"]
                            publish_time = feed["post"]["publish_time"]
                            publictime2 = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(publish_time))
                            # normal = feed["post"]["category"]["normal"]
                            # normal_hl=feed["post"]["category"]["normal_hl"]
                            news_link = "http://app3.qdaily.com/app3/articles/"+str(news_id)+".html"
                            insertsql = "replace into " + content_table_name + " (publicTime, publicTimestamp,news_id, channelName, channelId, title, abstract, commentNum, newsLink) values( %s,%s,%s, %s, %s, %s, %s, %s, %s)"
                            self.cursor.execute(insertsql, (
                                                    publictime2, publish_time,news_id, channelName, channelId, title, abstract, commentNum, news_link))
                            self.con.commit()
                    if content.has_key("feeds"):
                        for feed in content["feeds"]:
                            news_id = feed["post"]["id"]
                            title = feed["post"]["title"]
                            # news_From = feed[""]
                            commentNum = feed["post"]["comment_count"]
                            abstract = feed["post"]["description"]
                            publish_time = feed["post"]["publish_time"]
                            publictime2 = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(publish_time))
                            # normal = feed["post"]["category"]["normal"]
                            # normal_hl=feed["post"]["category"]["normal_hl"]
                            news_link = "http://app3.qdaily.com/app3/articles/" + str(news_id) + ".html"
                            insertsql = "replace into " + content_table_name + " (publicTime, publicTimestamp,news_id, channelName, channelId, title, abstract, commentNum, newsLink) values(%s,%s, %s, %s, %s, %s, %s, %s, %s)"
                            self.cursor.execute(insertsql, (
                                publictime2, publish_time,news_id, channelName, channelId, title, abstract, commentNum, news_link))
                            self.con.commit()


                    # for feed in content["feeds"]:
                    #     news_id = feed["id"]
                    #     title = feed["title"]
                    #     # news_From = feed[""]
                    #     commentNum=feed["comment_count"]
                    #     abstract = feed["description"]
                    #     publish_time = feed["publish_time"]



                    # insertsql = "replace into " + content_table_name + " (news_id, channelName, channelId, title, newsFrom, commentNum, Iner_use5, Iner_use6) values(%s, %s, %s, %s, %s, %s, %s, %s)"
                    #                         self.cursor.execute(insertsql, (
                    #                         news_id, channelName, channelId, title, newsFrom, commentNum, links_type, links_url))
                    #                         self.con.commit()
                # counter+=1










    # def GetNewsList(self, channelId, channelName):
    #
    #     url_para = self.news_list_param
    #     querystring = urllib.urlencode(url_para)
    #     # print querystring
    #     page = 1
    #     go = True
    #     ErrorCount = 0
    #     existsCount = 0
    #     url = ""
    #     while go:
    #         if channelId == 'SYLB10':
    #             url = self.toutiao_list_url + str(page) + '&newShowType=1&' + querystring
    #
    #         elif channelId == 'CJ33':
    #             url = self.cj_list_url + str(page) + '&' + querystring
    #
    #         if url:
    #             print url
    #             print ("=" * 50)
    #         else:
    #             return False
    #
    #         try:
    #             if ErrorCount > 20:
    #                 go = False
    #             response = urllib2.urlopen(url)
    #             content = json.loads(response.read())
    #             response.close()
    #         except urllib2.HTTPError, e:
    #             print "Error Code:", e.code
    #             ErrorCount += 1
    #         except urllib2.URLError, e:
    #             print "Error Reason:", e.reason
    #             ErrorCount += 1
    #         except:
    #             print "Other Error"
    #             ErrorCount += 1
    #             pass
    #         else:
    #             ErrorCount = 0
    #             if not content:  # list为空时，相当于false
    #                 break
    #
    #             # 判断是否为第一页
    #             if page == 1:
    #                 try:
    #                     for eachNews in content[0]["slides"]:
    #                         news_id = eachNews["documentId"]
    #                         title = eachNews.get('title')
    #                         newsFrom = eachNews.get('source')
    #                         commentNum = eachNews.get('commentsall', 0)
    #                         links_type = eachNews['link']['type']
    #                         links_url = eachNews['link']['url']
    #
    #                         if links_type != 'slide':
    #                             insertsql = "replace into " + content_table_name + " (news_id, channelName, channelId, title, newsFrom, commentNum, Iner_use5, Iner_use6) values(%s, %s, %s, %s, %s, %s, %s, %s)"
    #                             self.cursor.execute(insertsql, (
    #                             news_id, channelName, channelId, title, newsFrom, commentNum, links_type, links_url))
    #                             self.con.commit()
    #                         else:
    #                             pass
    #                 except:
    #                     try:
    #                         for eachNews in content[1]["item"]:
    #                             news_id = eachNews["documentId"]
    #                             title = eachNews.get('title')
    #                             # newsFrom = eachNews.get('source')
    #                             commentNum = eachNews.get('commentsall', 0)
    #                             links_type = eachNews['link']['type']
    #                             links_url = eachNews['link']['url']
    #
    #                             if links_type != 'slide':
    #                                 insertsql = "replace into " + content_table_name + " (news_id, channelName, channelId, title, commentNum, Iner_use5, Iner_use6) values(%s, %s, %s, %s, %s, %s, %s)"
    #                                 self.cursor.execute(insertsql, (
    #                                 news_id, channelName, channelId, title, commentNum, links_type, links_url))
    #                                 self.con.commit()
    #                             else:
    #                                 pass
    #                     except:
    #                         pass
    #
    #             else:
    #                 pass
    #
    #             # ======================================
    #             for eachNews in content[0]["item"]:
    #                 if existsCount > 30:
    #                     print 'existsCount', existsCount
    #                     go = False
    #                     break
    #                 news_id = eachNews["documentId"]
    #                 # print news_id
    #                 query = self.cursor.execute(
    #                     "select * from " + content_table_name + " where news_id ='" + news_id + "'")
    #                 if query > 0:
    #                     existsCount += 1
    #                 else:
    #                     existsCount = 0
    #                     title = eachNews.get('title')
    #                     # print title
    #                     # =============
    #                     newsFrom = eachNews.get('source')
    #
    #                     commentNum = eachNews.get('commentsall', 0)
    #                     # links_type = eachNews['links'][0]['type']
    #                     # links_url = eachNews['links'][0]['url']
    #                     links_type = eachNews['link']['type']
    #                     links_url = eachNews['link']['url']
    #                     # print commentNum
    #                     # print link_type
    #                     # print link_url
    #                     # print ("="*100)
    #
    #                     if links_type != 'slide':
    #                         insertsql = "replace into " + content_table_name + " (news_id, channelName, channelId, title, newsFrom, commentNum, Iner_use5, Iner_use6) values(%s, %s, %s, %s, %s, %s, %s, %s)"
    #                         self.cursor.execute(insertsql, (
    #                         news_id, channelName, channelId, title, newsFrom, commentNum, links_type, links_url))
    #                         self.con.commit()
    #                     else:
    #                         pass
    #
    #             if page >= 3:
    #                 go = False
    #             page += 1
    #             # break


if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    newsIfeng = NewsListCollector()
    newsIfeng.dbInit()
    newsIfeng.Starter()
    newsIfeng.dbClose()