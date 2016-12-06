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
import  sys
import  re

reload(sys)
sys.setdefaultencoding("utf-8")

warnings.filterwarnings("ignore", category=Mysql.Warning)

os.environ['TZ'] = 'Asia/Chongqing'
time.tzset()

content_table_name = "zhihu_content_news_" + time.strftime("%Y_%m")


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
        data = json.loads(data)
        self.news_list_param = data["news_list_param"]
        self.zhihu_list_url = data["zhihu_list_url"]



class NewsContentCollector(object):
    """docstring for NewsContentCollector"""

    # def __init__(self, arg):
    # 	super(NewsContentCollector, self).__init__()
    # 	self.arg = arg

    def __init__(self):
        parse_api = ParseConf()
        self.news_list_param = parse_api.news_list_param
        self.zhihu_list_url = parse_api.zhihu_list_url
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

    def dbClose(self):
        print "enter dbclose"
        self.cursor.close()  # 释放游标
        self.con.commit()  # 提交数据
        self.con.close()  # 关闭数据库

    def GetSpecialNewsList(self, topicUrl, channelName, channelId, publicTime):

        url = topicUrl
        print "Speciallist url*****", url
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
        except:
            pass
        else:
            textbody = content["body"]
            newsLink = content["share_url"]
            news_id= content["id"]
            url_extra = self.zhihu_list_url+"story-extra/"+str(news_id)
            title = content["title"]
            type = content["type"]
            ga_prefix = content["ga_prefix"]
            try:
                extra_response = urllib2.urlopen(url_extra)
                extra_content = json.loads(extra_response.read())
                extra_response.close()
                page = etree.HTML(str(textbody).lower().decode("utf-8"))

                textbody = re.sub("\s", "", str(page.xpath("string(//div)")))
                abstract = textbody[0:80]
            except urllib2.HTTPError, e:
                print "Error Code:", e.code
                pass
            except urllib2.URLError, e:
                print "Error Reason:", e.reason
                pass
            else:

                # print abstract
                comments_Num = extra_content["comments"]
                insertsql = "replace into " + content_table_name + "(abstract, Iner_use1, news_id, channelName, channelId, title, newsLink, publicTime,publicTimestamp, content, commentNum, Iner_use5, Iner_use6, Iner_use4) values(%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                self.cursor.execute(insertsql, (
                    abstract,ga_prefix,news_id, channelName, channelId, title,  newsLink, publicTime,int(time.mktime(time.strptime(str(publicTime), "%Y-%m-%d"))),textbody,
                    comments_Num, type, topicUrl, 1))
                self.con.commit()



    def Starter(self):
        query = self.cursor.execute(
            "select news_id, Iner_use5, Iner_use6 , channelName, channelId, publicTime from " + content_table_name + " where Iner_use4 = 0")
        print query
        if query:
            rows = self.cursor.fetchall()
            for row in rows:
                news_id = row[0]
                links_type = row[1]
                links_url = row[2]
                channelName = row[3]
                channelId = row[4]
                publicTime= row[5]
                if links_type == '0':
                    # 专题
                    self.GetSpecialNewsList(links_url, channelName, channelId, publicTime)
                    self.cursor.execute(
                        "update " + content_table_name + " set Iner_use4 = 1 where news_id = '" + news_id + "'")
                    self.con.commit()
                    pass
                elif links_type == 'slide':
                    # self.GetSlideContent(news_id ,links_url)
                    # 图集
                    pass
                elif links_type == 'sportTopic':
                    # 体育专题
                    pass
                else:
                    print links_type
                    self.GetNewsContent(news_id)


if __name__ == '__main__':
    socket.setdefaulttimeout(20)
    ifeng = NewsContentCollector()
    ifeng.dbInit()
    ifeng.Starter()
    ifeng.dbClose()
