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

content_table_name = "ifeng_content_news_" + time.strftime("%Y_%m")


class ParseConf(object):
    """
    Parse Conf File
    """

    def __init__(self):
        # conf_file = "/home/appnews/ifeng/news360.conf"
        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/appnews/ifeng.conf"
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
        cfgFileObj = open('/home/hadoop/bigdata/workspacepython/bigdata/tes1/appnews/DataBase.conf', 'r')
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

    def GetSpecialNewsList(self, topicUrl, channelName, channelId):

        url = topicUrl + "&" + self.querystring
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
            for eachSubject in content["body"]["subjects"]:  # 遍历subjects
                if eachSubject.has_key("podItems"):
                    # subject有podItems这key且对应的list不为空
                    if eachSubject["podItems"]:
                        for eachpodItem in eachSubject["podItems"]:
                            # 遍历每个subjects中的podItem
                            if eachpodItem.has_key("documentId"):
                                news_id = eachpodItem["documentId"]
                                commentNum = eachpodItem["commentCount"]
                                Iner_use5 = eachpodItem["links"][0]["type"]
                                Iner_use6 = eachpodItem["links"][0]["url"]
                                self.GetSpecialNewsContent(news_id, commentNum, Iner_use5, Iner_use6, channelName,
                                                           channelId)

    def GetSpecialNewsContent(self, news_id, commentNum, Iner_use5, Iner_use6, channelName, channelId):

        url = "http://api.3g.ifeng.com/ipadtestdoc?android=1&imcp_topic=json&aid=" + news_id[
                                                                                     5:] + '&' + self.querystring
        print "specialnews url", url
        try:
            # time.sleep(1)
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
            if content:
                body = content.get('body')
                publicTime = body.get('editTime')
                if publicTime:
                    publicTimestamp = time.mktime(time.strptime(publicTime, "%Y/%m/%d %H:%M:%S"))
                    newsFrom = body.get('source')
                    title = body.get('title')
                    textbody = body.get('text')
                    newsLink = body.get('commentsUrl')
                    insertsql = "replace into " + content_table_name + "(news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, content, commentNum, Iner_use5, Iner_use6, Iner_use4) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    self.cursor.execute(insertsql, (
                    news_id, channelName, channelId, title, newsFrom, newsLink, publicTime, publicTimestamp, textbody,
                    commentNum, Iner_use5, Iner_use6, 1))
                    self.con.commit()
                else:
                    pass

    def GetNewsContent(self, news_id):
        url = "http://api.iclient.ifeng.com/ipadtestdoc?aid=" + news_id + "&" + self.querystring
        print url
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
            if content:
                body = content["body"]["text"].encode('utf8')
                body = Mysql.escape_string(body)
                publicTime = content["body"].get('editTime').encode('utf8')
                publicTimestamp = time.mktime(time.strptime(publicTime, "%Y/%m/%d %H:%M:%S"))
                newsLink = content["body"]["commentsUrl"].encode('utf8')
                updateSql = "update  %s set newsLink = '%s', publicTime = '%s', publicTimestamp = %d ,content = '%s', Iner_use4 = %d where news_id = '%s'" % (
                content_table_name, newsLink, publicTime, publicTimestamp, body, 1, news_id.encode('utf8'))
                self.cursor.execute(updateSql)
                self.con.commit()

    def GetSlideContent(self, news_id, url):

        url = url + "&" + self.querystring
        print "slide news", url
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
            if content:
                body = content.get('body')
                publicTime = body.get('editTime').encode('utf8')
                publicTimestamp = time.mktime(time.strptime(publicTime, "%Y/%m/%d %H:%M:%S"))
                newsLink = body.get('commentsUrl').encode('utf8')
                newsLink = Mysql.escape_string(newsLink)
                slides = body.get('slides')
                textbody = "<img src='"
                for slide in slides:
                    desc = slide.get('description').encode('utf8')
                    imgUrl = slide.get('image').encode('utf8')
                    textbody += imgUrl + "'><p>" + desc + "</p><img src='"
                textbody = Mysql.escape_string(textbody[:-10])
                updateSql = "update  %s set newsLink = '%s', publicTime = '%s', publicTimestamp = %d ,content = '%s', Iner_use4 = %d where news_id = '%s'" % (
                content_table_name, newsLink, publicTime, publicTimestamp, textbody, 1, news_id.encode('utf8'))
                self.cursor.execute(updateSql)
                self.con.commit()

    def Starter(self):
        query = self.cursor.execute(
            "select news_id, Iner_use5, Iner_use6 , channelName, channelId from " + content_table_name + " where Iner_use4 = 0")
        print query
        if query:
            rows = self.cursor.fetchall()
            for row in rows:
                news_id = row[0]
                links_type = row[1]
                links_url = row[2]
                channelName = row[3]
                channelId = row[4]
                if links_type == 'topic2':
                    # 专题
                    self.GetSpecialNewsList(links_url, channelName, channelId)
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