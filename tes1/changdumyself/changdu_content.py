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
from lxml import  etree
import  gzip
import  StringIO

reload(sys)
sys.setdefaultencoding( "utf-8" )

warnings.filterwarnings("ignore", category=Mysql.Warning)

os.environ['TZ'] = 'Asia/Chongqing'
time.tzset()

content_table_name = "changdu_content_news_" + time.strftime("%Y_%m")


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
        data = json.loads(data)
        self.news_list_param = data["news_list_param"]


class NewsContentCollector(object):
    """docstring for NewsContentCollector"""

    # def __init__(self, arg):
    # 	super(NewsContentCollector, self).__init__()
    # 	self.arg = arg

    def __init__(self):
        parse_api = ParseConf()
        self.news_list_param = parse_api.news_list_param
        # print self.news_list_param
        self.channel_info_dict = {}
        self.headers = {
            "Connection": "Keep-Alive",
            "Host": "api.iclient.ifeng.com",
            "User-Agent": "Dalvik/1.6.0 (Linux; U; Android 4.2.2; Google Nexus 4 - 4.2.2 - API 17 - 768x1280 Build/JDQ39E)"
        }

        self.querystring = urllib.urlencode(self.news_list_param)

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

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def GetSpecialNewsList(self, topicUrl, channelName, channelId, news_id, title):
        url = "http://interfacev5.vivame.cn/x1-interface-v5/json/getnews.json?platform=android&installversion=6.2.2.2&channelno=AZWMA2320480100&mid=5284047f4ffb4e04824a2fd1d1f0cd62&uid=3125&sid=08524612-6825-46e8-83f4-5c70f4a99f56&type=0&id="+news_id+"&tagid="+channelId+"&pushtype=0"
        # url2 = Iner_use6
        print "Speciallist url*****", url
        # print  url2
        try:
            response = urllib2.urlopen(url)
            content = json.loads(response.read())
            response.close()
            # print  content
            # response2 = urllib2.urlopen(url)
            # # content2 = json.loads(response.read())
            # h = response2.read()
            # compressedstream = StringIO.StringIO(h)
            # gzipper = gzip.GzipFile(fileobj=compressedstream)
            # datas = json.loads(gzipper.read())
            # print  datas
            response2 = urllib2.urlopen(topicUrl)
            page =   etree.HTML(response2.read())
            response2.close()
            # print  page.text()
            # print page.xpath("string(//body)")

        except urllib2.HTTPError, e:
            print "Error Code:", e.code
            pass
        except urllib2.URLError, e:
            print "Error Reason:", e.reason
            pass
        except:
            pass
        else:
            data = content["data"]
            priurl = data["priurl"]
            source = data["source"]
            publicTimestamp = data["time"]
            publicTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(publicTimestamp / 1000.0))
            url = data["url"]
            content = page.xpath("string(//body)")

            # start = content.index(";")
            try:
                abstract = content[18+1:60]
            except:
                abstract = ""
            insertsql = "replace into " + content_table_name + "(abstract, news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, Iner_use4, content) values(%s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(insertsql, (
                abstract, news_id, channelName, channelId, title, source, topicUrl, publicTime, publicTimestamp,
                 1,content[18:]))
            self.con.commit()






    def Starter(self):
        query = self.cursor.execute(
            "select news_id,channelName, channelId ,newsLink ,title from " + content_table_name + " where Iner_use4 = 0")
        print query
        if query:
            rows = self.cursor.fetchall()
            for row in rows:
                news_id = row[0]
                channelName = row[1]
                channelId = row[2]
                links_url = row[3]
                title = row[4]
                # Iner_use6 = row[5]
                self.GetSpecialNewsList(links_url, channelName, channelId, news_id, title)
                self.cursor.execute(
                    "update " + content_table_name + " set Iner_use4 = 1 where news_id = '" + news_id + "'")
                self.con.commit()





if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    ifeng = NewsContentCollector()
    ifeng.dbInit()
    ifeng.Starter()
    ifeng.dbClose()
    # url = "http://interfacev5.vivame.cn/x1-interface-v5/json/getnews.json?platform=android&installversion=6.2.2.2&channelno=AZWMA2320480100&mid=5284047f4ffb4e04824a2fd1d1f0cd62&uid=3125&sid=08524612-6825-46e8-83f4-5c70f4a99f56&type=0&id=147918793391593506&tagid=6&pushtype=0"
    # response = urllib2.urlopen(url)
    # content = json.loads(response.read())
    # response.close()
    # data = content["data"]
    # priurl = data["priurl"]
    # source = data["source"]
    # publicTimestamp = data["time"]
    # publicTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(publicTimestamp / 1000.0))
    # url = data["url"]
    # print priurl
    # print source
    # print time
    # print url
    # print  publicTime
