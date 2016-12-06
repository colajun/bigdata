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

content_table_name = "rmr_content_news_" + time.strftime("%Y_%m")
rmr_news_table_create = "CREATE TABLE if not exists " + content_table_name + "(\
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
        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/remingmyself/rmr.conf"
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

        self.rmr_list_url = data["rmr_list_url"]
        # self.cj_list_url = data["cj_list_url"]
        self.news_list_param = data["news_list_param"]


class NewsListCollector(object):
    """docstring for NewsListCollector"""

    # def __init__(self, arg):
    # 	super(NewsListCollector, self).__init__()
    # 	self.arg = arg

    def __init__(self):

        parse_api = ParseConf()
        self.rmr_list_url = parse_api.rmr_list_url
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
        self.cursor.execute(rmr_news_table_create)

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def Starter(self):
        self.cursor.execute("select channelId , channelName from channellist where appName = 'rmr' and recommend > 0")
        rows = self.cursor.fetchall()
        for row in rows:
            channelId = row[0]
            channelName = row[1]
            print "channelId ===================", channelId
            self.GetNewsList(channelId, channelName)

    def GetNewsList(self, channelId, channelName):
        url_para = self.news_list_param
        querystring = urllib.urlencode(url_para)
        page = 1
        go = True
        ErrorCount = 0
        existsCount = 0
        url = ""
        while go:
            if channelId == '3':
                url = self.rmr_list_url +channelId+"&"+ querystring
            elif channelId == '4':
                url = self.rmr_list_url + channelId + '&' + querystring
            elif channelId == '5':
                url = self.rmr_list_url+channelId + '&'+querystring
            elif channelId == '10':
                url = self.rmr_list_url+channelId + '&'+querystring
            elif channelId == '11':
                url = self.rmr_list_url+channelId +'&'+querystring
            elif channelId == '56':
                url = self.rmr_list_url + channelId +'&'+querystring
            elif channelId == '57':
                url = self.rmr_list_url+ channelId +'&'+querystring
            elif channelId == '58':
                url = self.rmr_list_url +channelId + '&'+querystring
            elif channelId == '59':
                url = self.rmr_list_url + channelId +'&'+querystring
            elif channelId == '60':
                url = self.rmr_list_url+channelId+'&'+querystring
            elif channelId == '61':
                url = self.rmr_list_url+channelId +'&'+querystring
            elif channelId == '64':
                url = self.rmr_list_url + channelId + '&'+querystring

            if url:
                print url
                print ("=" * 50)
            else:
                return False

            try:
                if ErrorCount > 20:
                    go = False
                response = urllib2.urlopen(url)
                content = json.loads(response.read())
                response.close()
            except urllib2.HTTPError, e:
                print "Error Code:", e.code
                ErrorCount += 1
            except urllib2.URLError, e:
                print "Error Reason:", e.reason
                ErrorCount += 1
            except:
                print "Other Error"
                ErrorCount += 1
                pass
            else:
                if not content:  # list为空时，相当于false
                    break

                try:
                    for eachGroup in content["data"]:
                        for eachTitle in eachGroup["group_data"]:
                            news_id = eachTitle["id"]
                            title = eachTitle["title"]
                            newsFrom = eachTitle["copyfrom"]
                            commentNum = eachTitle.get("comment_count", 0)
                            viewType = eachTitle["view_type"]
                            news_datetime = eachTitle["news_datetime"]
                            news_timestamp = eachTitle["news_timestamp"]
                            share_link = eachTitle["share_url"]
                            share_logo = eachTitle["share_logo"]
                            insertsql = "replace into " + content_table_name + "(news_id, channelName, channelId, title, newsFrom, commentNum, Iner_use5, Iner_use6, publicTime, publicTimestamp, newsLink) values(%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)"
                            if share_link != None and  len(news_id) > 15:
                                self.cursor.execute(insertsql, (
                                    news_id, channelName, channelId, title, newsFrom, commentNum, viewType,
                                    "http://rmrbapi.people.cn/content/getdetail?articleid=" + news_id + "&categoryid=" + channelId,
                                    news_datetime, news_timestamp, share_link))
                                self.con.commit()
                            if share_link == None and share_logo != None and len(news_id) > 15:
                                self.cursor.execute(insertsql, (
                                    news_id, channelName, channelId, title, newsFrom, commentNum, viewType,
                                    "http://rmrbapi.people.cn/content/getdetail?articleid=" + news_id + "&categoryid=" + channelId,
                                    news_datetime, news_timestamp, share_logo))
                                self.con.commit()
                except KeyError, e:
                    pass



                go = False

                # 判断是否为第一
                #  break



if __name__ == '__main__':
    # url = "http://rmrbapi.people.cn/content/getcontentlist?categoryid=3&isoCC=us&device_size=768.0x1184.0&device_model=Genymotion-CustomPhone-4.4.4-API19-768x1280&categorytype=normal&MCC=310&client_ver=5.3.3&channel_num=baidu&platform=android&network_state=wifi&device_product=Genymotion&timestamp=0&udid=54c18364378d0ae3fef9a0a1f29bb80e&device_os=4.4.4&MNC=26&systype=cms"

    # response = urllib2.urlopen(url)
    # content = json.loads(response.read())
    # print  content
    # response.close()
    socket.setdefaulttimeout(20)
    newsIfeng = NewsListCollector()
    newsIfeng.dbInit()
    newsIfeng.Starter()
    newsIfeng.dbClose()