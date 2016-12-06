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
import  gzip
import  StringIO

warnings.filterwarnings("ignore", category=Mysql.Warning)
os.environ['TZ'] = 'Asia/Chongqing'
time.tzset()

content_table_name = "zaker_content_news_" + time.strftime("%Y_%m")
zaker_news_table_create = "CREATE TABLE if not exists " + content_table_name + "(\
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
						  `content` longtext,\
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
        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/zapermyself/zaper.conf"
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

        self.hotphone_list_url = data["hotphone_list_url"]
        self.news_list_param = data["news_list_param"]


class NewsListCollector(object):
    """docstring for NewsListCollector"""

    # def __init__(self, arg):
    # 	super(NewsListCollector, self).__init__()
    # 	self.arg = arg

    def __init__(self):

        parse_api = ParseConf()
        self.hotphone_list_url = parse_api.hotphone_list_url
        self.news_list_param = parse_api.news_list_param
        self.channel_info_dict = {}

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
        self.cursor.execute(zaker_news_table_create)

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def Starter(self):
        self.cursor.execute("select channelId , channelName from channellist where appName = 'zaker' and recommend > 0")
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

        if channelId == 'hot':
            # url = self.hotphone_list_url+ querystring
            url = "http://hotphone.myzaker.com/daily_hot_new.php?_appid=AndroidPhone&_bsize=720_1280&_city=chengdu&_dev=28&_lat=4.9E-324&_lng=4.9E-324&_mac=08%3A00%3A27%3A5b%3Aa9%3Afc&_mcode=5F25E246&_net=wifi&_nudid=9178e7896f9a59b1&_os=4.4.4_CustomPhone-4.4.4-API19-768x1280&_os_name=CustomPhone-4.4.4-API19-768x1280&_udid=4.4.4_CustomPhone-4.4.4-API19-768x1280.08%3A00%3A27%3A5b%3Aa9%3Afc&_v=7.0.2&_version=7.02"
        # elif channelId == 'CJ33':
        #     url = self.cj_list_url + str(page) + '&' + querystring

        counter = 0
        datas = {}
        while(counter < 30):
            if url:
                print url
                print ("=" * 50)
            else:
                return False

            try:
                req = urllib2.Request(url)
                req.add_header('User-agent', newsIfeng.headers.get("User-agent"))
                response = urllib2.urlopen(req)
                # content = json.loads(response.read())
                # response.close()
                h = response.read()
                compressedstream = StringIO.StringIO(h)
                gzipper = gzip.GzipFile(fileobj=compressedstream)
                datas = json.loads(gzipper.read())
            except urllib2.HTTPError, e:
                print "Error Code:", e.code
            except urllib2.URLError, e:
                print "Error Reason:", e.reason
            except:
                print "Other Error"
                pass
            else:
                ErrorCount = 0
                if not datas:  # list为空时，相当于false
                    return
            url = datas["data"]["info"]["next_url"]+urllib.urlencode(self.news_list_param)
            newsFrom = ""
            for article in datas["data"]["articles"]:
                # app_ids = article["app_ids"]
                try:
                    newsFrom = article["auther_name"]
                except KeyError, e:
                    pass
                date = article["date"]
                links_url = article["full_url"]
                # is_full = article["is_full"]
                # list_dtime = article["list_dtime"]
                news_id = article["pk"]
                title = article["title"]
                tmpurl = article["url"]
                # weburl = article["weburl"]
                insertsql = "replace into " + content_table_name + " (publicTimestamp, publicTime, newsLink, news_id, channelName, channelId, title, newsFrom,  Iner_use6) values(%s, %s, %s, %s, %s, %s, %s,%s, %s)"
                self.cursor.execute(insertsql, (
                int(time.mktime(time.strptime(str(date), "%Y-%m-%d %H:%M:%S"))),date,tmpurl, news_id, channelName, channelId, title, newsFrom,  links_url))
                self.con.commit()
            counter+= 1
            time.sleep(3)











if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    newsIfeng = NewsListCollector()
    newsIfeng.dbInit()
    newsIfeng.Starter()
    newsIfeng.dbClose()

    # url = "http://hotphone.myzaker.com/daily_hot_new.php?_appid=AndroidPhone&_bsize=720_1280&_city=chengdu&_dev=28&_lat=4.9E-324&_lng=4.9E-324&_mac=08%3A00%3A27%3A5b%3Aa9%3Afc&_mcode=5F25E246&_net=wifi&_nudid=9178e7896f9a59b1&_os=4.4.4_CustomPhone-4.4.4-API19-768x1280&_os_name=CustomPhone-4.4.4-API19-768x1280&_udid=4.4.4_CustomPhone-4.4.4-API19-768x1280.08%3A00%3A27%3A5b%3Aa9%3Afc&_v=7.0.2&_version=7.02"

    # url = "http://hotphone.myzaker.com/daily_hot_new.php?"+urllib.urlencode(newsIfeng.news_list_param)
    # response = urllib2.urlopen(url)
    # content = json.loads(response.read())
    # response.close()
    # print  content

    # req = urllib2.Request(url)
    # req.add_header('User-agent', newsIfeng.headers.get("User-agent"))
    # response = urllib2.urlopen(req)
    # # content = json.loads(response.read())
    # # response.close()
    # h = response.read()
    # compressedstream = StringIO.StringIO(h)
    # gzipper = gzip.GzipFile(fileobj=compressedstream)
    # data = json.loads(gzipper.read())
    #     news_id, channelName, channelId, title, newsFrom, commentNum, links_type, links_url))
    # for article in data["data"]["articles"]:
    #     # app_ids = article["app_ids"]
    #     newsFrom = article["auther_name"]
    #     # date = article["date"]
    #     links_url = article["full_url"]
    #     # is_full = article["is_full"]
    #     # list_dtime = article["list_dtime"]
    #     news_id= article["pk"]
    #     title = article["title"]
    #     url = article["url"]
    #     # weburl = article["weburl"]
    #     print  url
    # print data["data"]["info"]["next_url"]+urllib.urlencode(newsIfeng.news_list_param)

