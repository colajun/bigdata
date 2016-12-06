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

content_table_name = "zhihu_content_news_" + time.strftime("%Y_%m")
zhihu_news_table_create = "CREATE TABLE if not exists " + content_table_name + "(\
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
        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/zhihumysqlself/zhihu.conf"
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

        self.zhihu_list_url = data["zhihu_list_url"]
        self.news_list_param = data["news_list_param"]


class NewsListCollector(object):
    """docstring for NewsListCollector"""

    # def __init__(self, arg):
    # 	super(NewsListCollector, self).__init__()
    # 	self.arg = arg

    def __init__(self):

        parse_api = ParseConf()
        self.zhihu_list_url = parse_api.zhihu_list_url
        self.news_list_param = parse_api.news_list_param
        self.channel_info_dict = {}

        self.headers = {
            "Connection": "Keep-Alive",
            "Host": "api.iclient.ifeng.com",
            "User-Agent": "Dalvik/1.6.0 (Linux; U; Android 4.2.2; Google Nexus 4 - 4.2.2 - API 17 - 768x1280 Build/JDQ39E)"
        }

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
        self.cursor.execute(zhihu_news_table_create)

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def Starter(self):
        self.cursor.execute("select channelId , channelName from channellist where appName = 'zhihu' and recommend > 0")
        rows = self.cursor.fetchall()
        for row in rows:
            channelId = row[0]
            channelName = row[1]
            print "channelId ===================", channelId
            self.GetNewsList(channelId, channelName)

    def getDataAndInasert(self, url,channelId, channelName):
        try:
            response = urllib2.urlopen(url)
            content = json.loads(response.read())
            response.close()
        except urllib2.HTTPError, e:
            print "Error Code:", e.code
            pass
        except urllib2.URLError, e:
            print "Error Reason:", e.reason
            pass
        except UnboundLocalError,e:
            print "Error Reason:", e.reason
            pass
        except:
            print "Other Error"
            pass
        else:
            if not content:  # list为空时，相当于false
                return
        date = content["date"]
        for storie in content["stories"]:
            ga_prefix = storie["ga_prefix"]
            id = storie["id"]
            # images = storie["images"]
            title = storie["title"]
            type = storie["type"]
            insertsql = "replace into " + content_table_name + " (Iner_use1,news_id, channelName, channelId, title, Iner_use5, Iner_use6, publicTime) values(%s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(insertsql, (
                ga_prefix, id, channelName, channelId, title, type, self.zhihu_list_url+"story/"+str(id), str(date)[0:4]+"-"+str(date)[4:6]+"-"+str(date)[6:8]))
            self.con.commit()
        if content.has_key("top_stories"):
            for top_storie in content["top_stories"]:
                top_ga_prefix = top_storie["ga_prefix"]
                top_id = top_storie["id"]
                # top_images = top_storie["image"]
                top_title = top_storie["title"]
                top_type = top_storie["type"]
                # print
                insertsql = "replace into " + content_table_name + " (Iner_use1,news_id, channelName, channelId, title, Iner_use5, Iner_use6,publicTime) values(%s,%s, %s, %s, %s, %s, %s, %s)"
                self.cursor.execute(insertsql, (
                    top_ga_prefix, top_id, channelName, channelId, top_title, top_type, self.zhihu_list_url+"story/"+str(top_id), str(date)[0:4]+"-"+str(date)[4:6]+"-"+str(date)[6:8]))
                self.con.commit()
        return date



    def GetNewsList(self, channelId, channelName):

        url_para = self.news_list_param
        querystring = urllib.urlencode(url_para)
        # print querystring
        url = ""
        if channelId == 'stories':
            url = self.zhihu_list_url + channelId + "/latest"+querystring
            if url:
                print url
                print ("=" * 50)
            else:
                return False
            date = self.getDataAndInasert(url, channelId, channelName)

            date2=self.getDataAndInasert(self.zhihu_list_url+channelId+"/before/"+date+querystring, channelId, channelName)
            date3 = self.getDataAndInasert(self.zhihu_list_url+channelId+"/before/"+date2+querystring, channelId, channelName)
            date4 = self.getDataAndInasert(self.zhihu_list_url+channelId+"/before/"+date3+querystring, channelId, channelName)
            date5=self.getDataAndInasert(self.zhihu_list_url+channelId+"/before/"+date4+querystring, channelId, channelName)
            date6=self.getDataAndInasert(self.zhihu_list_url+channelId+"/before/"+date5+querystring, channelId, channelName)
            date7=self.getDataAndInasert(self.zhihu_list_url+channelId+"/before/"+date6+querystring, channelId, channelName)
            self.getDataAndInasert(self.zhihu_list_url+channelId+"/before/"+date7+querystring, channelId, channelName)


        elif channelId == 'CJ33':
            url = self.cj_list_url + '&'











                    # 判断是否为第一页


if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    newsIfeng = NewsListCollector()
    newsIfeng.dbInit()
    newsIfeng.Starter()
    newsIfeng.dbClose()
    # response = urllib2.urlopen("http://news-at.zhihu.com/api/4/stories/latest")
    # content = json.loads(response.read())
    # response.close()
    # for storie in content["stories"]:
    #     ga_prefix = storie["ga_prefix"]
    #     id = storie["id"]
    #     images = storie["images"]
    #     title = storie["title"]
    #     type = storie["type"]
    # if content.has_key("top_stories"):
    #     for top_storie in content["top_stories"]:
    #         top_ga_prefix = top_storie["ga_prefix"]
    #         top_id = top_storie["id"]
    #         top_images = top_storie["image"]
    #         top_title = top_storie["title"]
    #         top_type = top_storie["type"]
    #         print top_images

