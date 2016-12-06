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

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def GetSpecialNewsList(self, links_url, news_id, channelId,channelName,title, newsFrom, newsLink, publicTime, publicTimestamp,  commentsNum, Iner_use5):
        url = links_url+"&" + self.querystring
        print "Speciallist url*****", url
        if len(news_id) > 10:

            try:
                response = urllib2.urlopen(url)
                content = json.loads(response.read())
                contenttmp = content["data"]["content"]
                abstract = content["data"]["description"][0:50]
                response.close()
            except urllib2.HTTPError, e:
                print "Error Code:", e.code
                pass
            except urllib2.URLError, e:
                print "Error Reason:", e.reason
                pass
            except:
                pass
            else:
                # abstract = contenttmp[start:start+50]
                insertsql = "replace into " + content_table_name + "(Iner_use6, news_id, channelId,channelName,title, newsFrom, newsLink, publicTime, publicTimestamp, abstract, content, commentNum,Iner_use5,Iner_use4) values(%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                self.cursor.execute(insertsql, (
                    links_url, news_id, channelId, channelName, title, newsFrom, newsLink, publicTime, publicTimestamp,
                                    abstract, contenttmp, commentsNum, Iner_use5,1
                    ))
                self.con.commit()







    def Starter(self):
        query = self.cursor.execute(
            "select news_id ,Iner_use6 , channelName, channelId , title, newsFrom, newsLink, publicTime, publicTimestamp, commentNum, Iner_use5 from " + content_table_name + " where Iner_use4 = 0")
        print query
        if query:
            rows = self.cursor.fetchall()
            for row in rows:
                news_id = row[0]
                links_url = row[1]
                channelName = row[2]
                channelId = row[3]
                title = row[4]
                newsFrom= row[5]
                newsLink = row[6]
                publicTime=row[7]
                publicTimestamp = row[8]
                commentsNum= row[9]
                Iner_use5= row[10]


                self.GetSpecialNewsList(links_url, news_id, channelId,channelName,title, newsFrom, newsLink, publicTime, publicTimestamp, commentsNum,Iner_use5,)
                self.cursor.execute(
                    "update " + content_table_name + " set Iner_use4 = 1 where id = '" + news_id+"'" )
                self.con.commit()



if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    ifeng = NewsContentCollector()
    ifeng.dbInit()
    ifeng.Starter()
    ifeng.dbClose()
    # response = urllib2.urlopen("http://rmrbapi.people.cn/content/getdetail?articleid=1917100773245952_cms_1917100773245952&categoryi")
    # content = json.loads(response.read())
    # contenttmp = content["data"]["content"]
    # print  content["data"]["comment_count"]
    # response.close()

