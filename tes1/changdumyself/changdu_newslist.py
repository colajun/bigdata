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

warnings.filterwarnings("ignore", category=Mysql.Warning)
os.environ['TZ'] = 'Asia/Chongqing'
time.tzset()

content_table_name = "changdu_content_news_" + time.strftime("%Y_%m")
changdu_news_table_create = "CREATE TABLE if not exists " + content_table_name + "(\
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
        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/changdumyself/changdu.conf"
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

        self.changdu_list_url = data["changdu_list_url"]
        self.news_list_param = data["news_list_param"]


class NewsListCollector(object):
    """docstring for NewsListCollector"""

    # def __init__(self, arg):
    # 	super(NewsListCollector, self).__init__()
    # 	self.arg = arg

    def __init__(self):

        parse_api = ParseConf()
        self.changdu_list_url = parse_api.changdu_list_url
        self.news_list_param = parse_api.news_list_param
        self.channel_info_dict = {}

        self.headers = {
            "Connection": "Keep-Alive",
            "Host": "api.iclient.ifeng.com",
            "User-Agent": "Dalvik/1.6.0 (Linux; U; Android 4.2.2; Google Nexus 4 - 4.2.2 - API 17 - 768x1280 Build/JDQ39E)"
        }

    def dbInit(self):
        # cfgFileObj = open('/home/appnews/DataBase.conf', 'r')
        cfgFileObj = open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/changdumyself/dataBase.conf', 'r')
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
        self.cursor.execute(changdu_news_table_create)

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def Starter(self):
        self.cursor.execute("select channelId , channelName from channellist where appName = 'changdu' and recommend > 0")
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
        if channelId == '9001':
            url = self.changdu_list_url + str(9001) + '&' + querystring+"&t="+str(int(round(time.time() * 1000)))

        elif channelId == '12':
            url = self.changdu_list_url + str(12) + '&' + querystring+"&t=0"

        elif channelId == "143":
            url = self.changdu_list_url + str(143) + '&' + querystring+"&t=0"

        elif channelId == "6":
            url = self.changdu_list_url + str(143) + '&' + querystring+"&t=0"

        elif channelId == "40000":
            url = self.changdu_list_url + str(40000) + '&' + querystring+"&t=0"

        elif channelId == "9":
            url = self.changdu_list_url + str(9) + '&' + querystring+"&t=0"

        elif channelId == "140":
            url = self.changdu_list_url + str(140) + '&' + querystring+"&t=0"

        elif channelId == "45":
            url = self.changdu_list_url + str(45) + '&' + querystring+"&t=0"

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
            ErrorCount = 0
            if not content:  # list为空时，相当于false
               return
            for item in content["data"]["feedlist"]:
                news_id = item["items"][0]["url"]
                title = item["items"][0]["title"]
                newsLink = item["items"][0]["fileurl"]
                hot = item["items"][0].get("hot", 0)
                # print newsLink+".w=384&installversion=6.2.2.2&changeFontSize"
                insertsql = "replace into " + content_table_name + " (news_id, channelName, channelId, title,newsLink, Iner_use5) values( %s, %s, %s, %s, %s, %s)"
                self.cursor.execute(insertsql, (
                    news_id, channelName, channelId, title,  newsLink, hot))
                self.con.commit()








if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    newsIfeng = NewsListCollector()
    newsIfeng.dbInit()
    newsIfeng.Starter()
    newsIfeng.dbClose()
    # news_id, channelName, channelId, title, newsFrom, commentNum, links_type, links_url))
    # url = "http://interfacev5.vivame.cn/x1-interface-v5/json/newdatalist.json?platform=android&installversion=6.2.2.2&channelno=AZWMA2320480100&mid=5284047f4ffb4e04824a2fd1d1f0cd62&uid=3125&sid=08524612-6825-46e8-83f4-5c70f4a99f56&type=1&id=6&category=1&ot=0&nt=0&t=0"
    # response = urllib2.urlopen(url)
    # content = json.loads(response.read())
    # response.close()
    # for item in content["data"]["feedlist"]:
    #     news_id = item["items"][0]["url"]
    #     title = item["items"][0]["title"]
    #     newsLink = item["items"][0]["fileurl"]
    #     hot = item["items"][0].get("hot", 0)
    #     # desc = item["items"][0]["desc"]
    #     # print  desc
    #     print  "=================="






