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
import  re
import  sys

reload(sys)

sys.setdefaultencoding('utf-8')



warnings.filterwarnings("ignore", category=Mysql.Warning)

os.environ['TZ'] = 'Asia/Chongqing'
time.tzset()

content_table_name = "360news_content_news_" + time.strftime("%Y_%m")


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

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def GetSpecialNewsList(self, news_id,newsLink,topicUrl, channelName, channelId):

        url = topicUrl + "&fmt=json&url=" + newsLink
        print "Speciallist url*****", url
        try:
            response = urllib2.urlopen(url)
            content = json.loads(response.read())
            response.close()
            title = content["data"]["title"]
        except urllib2.HTTPError, e:
            print "Error Code:", e.code
            pass
        except urllib2.URLError, e:
            print "Error Reason:", e.reason
            pass
        except:
            pass
        else:

            newsFrom = content["data"]["source"]
            Iner_use6 = url
            publicTimestamp = content["data"]["time"]
            publicTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(publicTimestamp)))
            textbody = ""
            # print  publicTime
            for txt in content["data"]["content"]:
                if txt["type"] == "txt":
                    textbody = textbody + txt["value"]
            # end = textbody.index("。")
            try:
                abstract = textbody[0:50]
            except:
                abstract =""
            # abstract = re.sub("<.*?>", "", str(textbody)[0:200])
            insertsql = "replace into " + content_table_name + "(news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, content,  Iner_use6, Iner_use4, abstract) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(insertsql, (
                news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, textbody,
                Iner_use6, 1, abstract))
            self.con.commit()
            # print  abstract


    def Starter(self):
        query = self.cursor.execute(
            "select news_id, newsLink, Iner_use6 , channelName, channelId from " + content_table_name + " where Iner_use4 = 0")
        print query
        if query:
            rows = self.cursor.fetchall()
            for row in rows:
                news_id = row[0]
                newsLink = row[1]
                links_url = row[2]
                channelName = row[3]
                channelId = row[4]
                self.GetSpecialNewsList(news_id,newsLink,links_url, channelName, channelId)
                self.cursor.execute(
                    "update " + content_table_name + " set Iner_use4 = 1 where news_id = '" + news_id + "'")
                self.con.commit()


if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    ifeng = NewsContentCollector()
    ifeng.dbInit()
    ifeng.Starter()
    ifeng.dbClose()
