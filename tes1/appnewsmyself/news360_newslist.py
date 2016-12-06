#!/usr/bin/python
#_*_ coding: utf8 _*_
# coding=utf-8


import  sys, urllib, urllib2
import  MySQLdb as Mysql
import  simplejson as json
import  time
import  socket
import  warnings
import  os

warnings.filterwarnings("ignore", category=Mysql.Warning)
os.environ['TZ'] = 'Asia/Chongqing'
time.timezone

content_table_name = "news360_content_news_"+time.strftime("%Y_%m")

news360_news_table_create = "CREATE TABLE if not exists " + content_table_name + "(\
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

        conf_file = "/home/hadoop/bigdata/workspacepython/bigdata/tes1/appnewsmyself/news360.conf"
        if not os.path.exists(conf_file):
            print "Not Found Parse Conf File!"
            return  None
        else:
            self.conf_file = conf_file
            self.parse()

    def parse(self):

        fileHandle = open(self.conf_file)
        data = fileHandle.read()
        fileHandle.close()
        data =  json.loads(data)

        self.news360_list_url = data["news360_list_url"]
        self.news_list_param = data["news_list_param"]




class NewsListCollector(object):
    """
    docstring for NewsListCollector
    """

    def __init__(self):

        parse_api = ParseConf()
        self.news360_list_url = parse_api.news360_list_url
        self.news_list_param = parse_api.news_list_param
        self.channel_info_dict = {}

        self.headers = {
            "Connection": "Keep-Alive",
            "Host": "rmrbapi.people.cn",
            "User-Agent": "Dalvik/1.6.0 (Linux; U; Android 4.2.2; Google Nexus 4 - 4.2.2 - API 17 - 768x1280 Build/JDQ39E)"
        }

    def dbInit(self):
        cfgFileObj = open("/home/hadoop/bigdata/workspacepython/bigdata/tes1/appnews/DataBase.conf", "r")
        cfgContent = cfgFileObj.read()
        cfgFileObj.close()
        lineList = cfgContent.split("\n")
        hostName = lineList[0].split("=")[1]
        userName = lineList[1].split("=")[1]
        password = lineList[2].split("=")[1]
        selectDB = lineList[3].split("=")[1]
        self.con = Mysql.connect(host = hostName, user= userName, passwd = password, charset = "utf8")
        print "enter dbInit"
        self.cursor = self.con.cursor()
        self.cursor.execute("create database if not exists "+selectDB)
        self.con.select_db(selectDB)
        self.cursor.execute(news360_news_table_create)


    def dbClose(self):
        print "enter dbClose"
        self.cursor.close()
        self.con.commit()
        self.con.close()



    # def Starter(self):
    #     self.cursor.execute("select channelId, channelName from channellist where appName='pengpai' and recommend >0")
    #     rows = self.cursor.fetchall()
    #     for row in rows:
    #         channelId = row[0]
    #         channelName = row[1]
    #         print "channelId====================", channelId
    #         self.GetNewsList(channelId, channelName)



    def GetNewsList(self, channelId, channelName):
        url_para = self.news_list_param
        querystring = urllib.urlencode(url_para)
        go = True
        ErrorCount = 0
        existsCount = 0
        url = ""
        while go:
            url = self.news360_list_url

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
                ErrorCount = 0
                if not content:  # list为空时，相当于false
                    break

            try:
                for eachNews in content[0][""]:
                    title = eachNews.get("title")
                    newsFrom = eachNews.get("source")
                    print title
                    print newsFrom
            except:
                pass


if __name__ == "__main__":
    socket.setdefaulttimeout(20)
    news360 = NewsListCollector()
    news360.dbInit()
    news360.GetNewsList(23, 45)
    news360.dbClose()









