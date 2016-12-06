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
import sys
import  re
from lxml import  etree

reload(sys)
sys.setdefaultencoding('utf-8')

warnings.filterwarnings("ignore", category=Mysql.Warning)

os.environ['TZ'] = 'Asia/Chongqing'
time.tzset()

content_table_name = "zaker_content_news_" + time.strftime("%Y_%m")


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
            # "User-Agent": "Dalvik/1.6.0 (Linux; U; Android 4.2.2; Google Nexus 4 - 4.2.2 - API 17 - 768x1280 Build/JDQ39E)"
            "User-Agent": "Mozilla/5.0 (Linux; U; Android 4.4.4; zh-cn;Custom Phone - 4.4.4 - API 19 - 768x1280 Build/JOP40D; CyanogenMod-10.1) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30"

        }

        self.querystring = urllib.urlencode(self.news_list_param)

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

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def GetSpecialNewsList(self, topicUrl, channelName, channelId, news_id, title, newsFrom, newsLink, publicTime, publicTimestamp):

        url = topicUrl
        print "Speciallist url*****", url
        try:
            req = urllib2.Request(url)
            req.add_header('User-agent', self.headers.get("User-agent"))
            response = urllib2.urlopen(req)
            h = response.read()
            compressedstream = StringIO.StringIO(h)
            gzipper = gzip.GzipFile(fileobj=compressedstream)
            datas = json.loads(gzipper.read())
        except urllib2.HTTPError, e:
            print "Error Code:", e.code
            pass
        except urllib2.URLError, e:
            print "Error Reason:", e.reason
            pass
        except:
            pass
        else:
            textbody = datas["data"]["content"]
            try:
                # start = str(textbody).index("<p>")
                # abstract = str(textbody)[start:start+100]
                # abstract = re.sub(r'{.*}', "", str(textbody))
                page =etree.HTML(str(textbody).lower().decode("utf-8"))
                abstract = page.xpath("string(//body)")[0:50]
            except:
                abstract = ""
            # print  abstract

            # insertsql = "replace into " + content_table_name + "(news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, content, commentNum, Iner_use5, Iner_use6, Iner_use4) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            # self.cursor.execute(insertsql, (
            # news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, textbody,
            # commentNum, Iner_use5, Iner_use6, 1))
            # self.con.commit()
            insertsql = "replace into " + content_table_name + "(news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, content,  Iner_use6, Iner_use4, abstract) values( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(insertsql, (
                news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, textbody,
                 topicUrl, 1,abstract))
            self.con.commit()




    def Starter(self):
        query = self.cursor.execute(
            "select news_id,  Iner_use6 , channelName, channelId,title, newsFrom, newsLink, publicTime, publicTimestamp from " + content_table_name + " where Iner_use4 = 0")
        print query
        if query:
            rows = self.cursor.fetchall()
            for row in rows:
                news_id = row[0]
                links_url = row[1]
                channelName = row[2]
                channelId = row[3]
                title = row[4]
                newsFrom = row[5]
                newsLink = row[6]
                publicTime = row[7]
                publicTimestamp = row[8]


                self.GetSpecialNewsList(links_url, channelName, channelId, news_id, title, newsFrom, newsLink, publicTime, publicTimestamp)
                self.cursor.execute(
                        "update " + content_table_name + " set Iner_use4 = 1 where news_id = '" + news_id + "'")
                self.con.commit()




if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    ifeng = NewsContentCollector()
    ifeng.dbInit()
    ifeng.Starter()
    ifeng.dbClose()
    # url = "http://iphone.myzaker.com/zaker/article_mongo.php?app_id=12&pk=582950b79490cb682100007b&m=1479131823"
    # req = urllib2.Request(url)
    # req.add_header('User-agent', ifeng.headers.get("User-agent"))
    # response = urllib2.urlopen(req)
    # h = response.read()
    # compressedstream = StringIO.StringIO(h)
    # gzipper = gzip.GzipFile(fileobj=compressedstream)
    # datas = json.loads(gzipper.read())
    # print datas["data"]["content"]