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

content_table_name = "360news_content_news_" + time.strftime("%Y_%m")
ifeng_news_table_create = "CREATE TABLE if not exists " + content_table_name + "(\
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
        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/360newsmyself/360news.conf"
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

        self.newsit_list_url = data["360newsit_list_url"]
        self.newsmilitery_list_url = data["360newsmilitery_list_url"]
        self.newscar_list_url = data["360newscar_list_url"]
        self.newsfun_list_url = data["360newsfun_list_url"]
        self.newssocial_list_url = data["360newssocial_list_url"]
        self.newshb_list_url = data["360newshb_list_url"]
        self.newsinternational_list_url = data["360newsinternational_list_url"]

        self.news_list_param = data["news_list_param"]


class NewsListCollector(object):
    """docstring for NewsListCollector"""

    # def __init__(self, arg):
    # 	super(NewsListCollector, self).__init__()
    # 	self.arg = arg

    def __init__(self):

        parse_api = ParseConf()
        self.newsit_list_url = parse_api.newsit_list_url
        self.newsmilitery_list_url = parse_api.newsmilitery_list_url
        self.newscar_list_url = parse_api.newscar_list_url
        self.newsfun_list_url = parse_api.newsfun_list_url
        self.newssocial_list_url = parse_api.newssocial_list_url
        self.newshb_list_url = parse_api.newshb_list_url
        self.newsinternational_list_url = parse_api.newsinternational_list_url
        self.news_list_param = parse_api.news_list_param
        self.channel_info_dict = {}

        self.headers = {
            "Connection": "Keep-Alive",
            "Host": "api.iclient.ifeng.com",
            "User-Agent": "Dalvik/1.6.0 (Linux; U; Android 4.2.2; Google Nexus 4 - 4.2.2 - API 17 - 768x1280 Build/JDQ39E)"
        }

    def dbInit(self):
        # cfgFileObj = open('/home/appnews/DataBase.conf', 'r')
        cfgFileObj = open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/360newsmyself/DataBase.conf', 'r')
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
        self.cursor.execute(ifeng_news_table_create)

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def Starter(self):
        self.cursor.execute("select channelId , channelName from channellist where appName = '360news' and recommend > 0")
        rows = self.cursor.fetchall()
        for row in rows:
            channelId = row[0]
            channelName = row[1]
            print "channelId ===================", channelId
            self.GetNewsList(channelId, channelName)

    def GetNewsList(self, channelId, channelName):

        url_para = self.news_list_param
        querystring = urllib.urlencode(url_para)
        # print querystring
        url = ""
        if channelId == 'it':
            url = self.newsit_list_url+ querystring

        elif channelId == 'militery':
            url = self.newsmilitery_list_url + querystring
        elif channelId == "car":
            url = self.newscar_list_url + querystring
        elif channelId == "fun":
            url = self.newsfun_list_url+querystring
        elif channelId == "social":
            url = self.newssocial_list_url + querystring
        elif channelId == "hb":
            url = self.newshb_list_url + querystring
        elif channelId == "international":
            url = self.newsinternational_list_url + querystring
        elif channelId == "domestic":
            url = self.newsinternational_list_url + querystring
        if url:
            print url
            print ("=" * 50)
        else:
            return False

        try:
            response = urllib2.urlopen(url)
            content = json.loads(response.read())
            response.close()
        except urllib2.HTTPError, e:
            print "Error Code:", e.code
        except urllib2.URLError, e:
            print "Error Reason:", e.reason
        except:
            print "Other Error"
            pass
        else:
            if not content:  # list为空时，相当于false
                return
        for number in range(len(content)):
            eachTheme = content[number]
            a = eachTheme["a"]
            c = eachTheme["c"]
            f= eachTheme["f"]
            hn = eachTheme["hn"]
            i = eachTheme["i"]
            id = eachTheme["id"]
            idx = eachTheme["idx"]
            k = eachTheme["k"]
            m = eachTheme["m"]
            n_t = eachTheme["n_t"]
            nid = eachTheme["nid"]
            p = eachTheme["p"]
            s = eachTheme["s"]
            t = eachTheme["t"]
            u = eachTheme["u"]
            zm = eachTheme["zm"]
            insertsql = "replace into " + content_table_name + " (newsLink,news_id, channelName, channelId, title, newsFrom,  Iner_use6) values(%s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(insertsql, (
            u, id, channelName, channelId, t, f,  zm))
            self.con.commit()


if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    news360news = NewsListCollector()
    news360news.dbInit()
    news360news.Starter()
    news360news.dbClose()
    # url = news360news.newsinternational_list_url+urllib.urlencode(news360news.news_list_param)
    # print url
    # response = urllib2.urlopen(url)
    # content = json.loads(response.read())
    # response.close()
    # for number in range(len(content)):
    #     eachTheme = content[number]
    #     a = eachTheme["a"]
    #     c = eachTheme["c"]
    #     hn = eachTheme["hn"]
    #     i = eachTheme["i"]
    #     id = eachTheme["id"]
    #     idx = eachTheme["idx"]
    #     k = eachTheme["k"]
    #     m = eachTheme["m"]
    #     n_t = eachTheme["n_t"]
    #     nid = eachTheme["nid"]
    #     p = eachTheme["p"]
    #     s = eachTheme["s"]
    #     t = eachTheme["t"]
    #     u = eachTheme["u"]
    #     zm = eachTheme["zm"]
    #     print u
    #     print zm
    #     print "======================="

