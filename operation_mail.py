#!/usr/bin/env python3
#-*- coding: utf-8 -*-
import datetime
import smtplib
import os
from email.mime.text import MIMEText
from email.utils import formataddr
from email.header import Header
import pymysql
import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import sys
import importlib
importlib.reload(sys)
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import datetime
#sys.setdefaultencoding('utf8')
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
class Operation_mail:
    '''获取链接'''
    def getconnect(self):
        # 数据库ip地址
        host="192.168.115.105"
        # 数据库名称
        db = "news_manage_test"
        # 登陆用户名
        user="root"
        # 登陆密码
        password="Hik12345+"
        # 获取上一天日期
        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today-oneday
        # 链接数据库
        db = pymysql.connect(host,user,password,db,port=3306)
        # 使用cursor()的方法获取游标
        cur=db.cursor()
        return cur,yesterday
    '''定义用户留存'''
    def remain(self,cur):
        sql  = 'select stat_date,sum(1_all) "a",sum(2_all) "b",sum(3_all) "c",sum(4_all) "d",sum(5_all) "e",sum(6_all) "f",sum(7_all) "g" from (SELECT stat_date,case idx when "1_day_save" then idx_value end "1_all", case idx when "2_day_save" then idx_value end "2_all", case idx when "3_day_save" then idx_value end "3_all", case idx when "4_day_save" then idx_value end "4_all", case idx when "5_day_save" then idx_value end "5_all", case idx when "6_day_save" then idx_value end "6_all", case idx when "7_day_save" then idx_value end "7_all" FROM `news_app_analysis` WHERE idx_group = "user_save") aa where aa.stat_date > date_sub(CURRENT_DATE,INTERVAL 9 DAY)GROUP BY stat_date '
        cur.execute(sql)
        results = cur.fetchall()
        df = pd.DataFrame([ij for ij in i] for i in results)
        df2 = df.replace([None], [' '])
        print(df2)
        return df2
    '''定义其它格式数据基础数据图表展示'''
    def data(selfs,cur):
        #sql = '''select a.dt, b.allDeviceNum , a.newUserNum , (b.androidNewDevice + b.iosNewDevice), b.androidNewDevice, b.iosNewDevice, b.startDevice, b.startAll, b.androidActivityDevice + b.iosActivityDevice, b.androidActivityDevice, b.iosActivityDevice, round(b.perDurationAll/b.allDeviceNum,2), round(b.perDurationAll/b.startAll,2), round(b.startAll/b.allDeviceNum,2) from (select DATE_FORMAT(date_sub(now(), interval 1 DAY),"%Y-%m-%d") dt,SUM(idx_value) newUserNum from news_app_analysis where stat_date = DATE_FORMAT(date_sub(now(), interval 1 DAY),"%Y-%m-%d 00:00:00") and  idx_group = 'day_user' ) a join ( select DATE_FORMAT(date_sub(now(), interval 1 DAY),"%Y-%m-%d") dt, SUM(CASE WHEN  idx = 'all' then idx_value else 0 end) allDeviceNum , SUM(CASE WHEN  idx = 'startAll' then idx_value else 0 end) startAll , SUM(CASE WHEN  idx = 'startDevice' then idx_value else 0 end) startDevice , SUM(CASE WHEN  idx = 'perDurationAll' then idx_value else 0 end) perDurationAll , SUM(CASE WHEN  idx = 'new' and channel = 'android' then idx_value else 0 end) androidNewDevice , SUM(CASE WHEN  idx = 'new' and channel = 'ios' then idx_value else 0 end) iosNewDevice , SUM(CASE WHEN  idx = 'activity' and channel = 'android' then idx_value else 0 end) androidActivityDevice , SUM(CASE WHEN  idx = 'activity' and channel = 'ios' then idx_value else 0 end) iosActivityDevice from news_app_analysis where stat_date = DATE_FORMAT(date_sub(now(), interval 1 DAY),"%Y-%m-%d 00:00:00") and idx_group = 'day_device' ) b ON a.dt = b.dt'''
        sql = '''select a.dt, b.allDeviceNum , a.newUserNum , b.allDeviceNum - (b.androidNewDevice + b.iosNewDevice) ,(b.androidNewDevice + b.iosNewDevice), b.androidNewDevice, b.iosNewDevice, b.startDevice, b.androidActivityDevice + b.iosActivityDevice, b.androidActivityDevice, b.iosActivityDevice, round(b.perDurationAll/b.allDeviceNum,2), round(b.perDurationAll/b.startAll,2), b.startAll,round(b.startAll/b.allDeviceNum,2) from (select DATE_FORMAT(date_sub(now(), interval 1 DAY),"%Y-%m-%d") dt,SUM(idx_value) newUserNum from news_app_analysis where stat_date = DATE_FORMAT(date_sub(now(), interval 7 DAY),"%Y-%m-%d 00:00:00") and  idx_group = 'day_user' ) a join ( select DATE_FORMAT(date_sub(now(), interval 1 DAY),"%Y-%m-%d") dt, SUM(CASE WHEN  idx = 'all' then idx_value else 0 end) allDeviceNum , SUM(CASE WHEN  idx = 'startAll' then idx_value else 0 end) startAll , SUM(CASE WHEN  idx = 'startDevice' then idx_value else 0 end) startDevice , SUM(CASE WHEN  idx = 'perDurationAll' then idx_value else 0 end) perDurationAll , SUM(CASE WHEN  idx = 'new' and channel = 'android' then idx_value else 0 end) androidNewDevice , SUM(CASE WHEN  idx = 'new' and channel = 'ios' then idx_value else 0 end) iosNewDevice , SUM(CASE WHEN  idx = 'activity' and channel = 'android' then idx_value else 0 end) androidActivityDevice , SUM(CASE WHEN  idx = 'activity' and channel = 'ios' then idx_value else 0 end) iosActivityDevice from news_app_analysis where stat_date = DATE_FORMAT(date_sub(now(), interval 7 DAY),"%Y-%m-%d 00:00:00") and idx_group = 'day_device' ) b ON a.dt = b.dt'''
        cur.execute(sql)
        results=cur.fetchall()
        df = pd.DataFrame([ij for ij in i] for i in results)
        df.rename(columns={0: '日期', 1: '总设备数', 2: '新注册用户数'}, inplace=True)
        # 建一个空的列表
        dt = []
        # 进行列转行
        df2 = df.stack(0)
        # 循环将数值加入到列表中
        df2.replace([None],[0])
        print(df2)
        for i in range(1,15):
            dt.append(df2[i])

        #print(dt)
        xlabel = [u"总设备数",u'新注册用户数',u'老设备数',u'新设备数',u'安卓新设备数',u'ios新设备数',u'ios新设备数',u'活跃设备数',u'安卓活跃设备数',u'ios活跃设备数',u'每人使用时长',u'每次使用时长',u'总启动次数',u'人均启动次数']
        s = pd.Series(dt,xlabel)
        s.plot(kind='bar',rot=30,grid=1,alpha=0.7,figsize=(12,8))
        plt.savefig("enable.png")

        plt.show()
        return df

    '''用户留存html 样式'''
    def remain_style(self,df1):
        # d为表格内容
        d1 = ''
        for i in range(len(df1)):
            d1 = d1 + """
            <tr>
                <td>""" + str(df1.index[i]) + """</td>
                <td >""" + str(df1.iloc[i][0]) + """</td>
                
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][1]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][2]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][3]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][4]) + """</h2></td>
                <td width="75" align="center"><h2> """ + str(df1.iloc[i][5]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][6]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][7]) + """</h2></td>
            </tr>"""

            html = """\
                   <head>
                   <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                   <body>
                   <div id="container">
                   <p><h2>展示用户七日留存数据</h2></p>
                   <div id="content">
                    <table width="55%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
                   <tr>
                     <td width="40"><strong>排序</strong></td>
                     <td width="50"><strong>日期</strong></td>

                     <td width="50" align="center"><strong>次日留存</strong></td>
                     <td width="50" align="center"><strong>二日留存</strong></td>
                     <td width="50" align="center"><strong>三日留存</strong></td>
                     <td width="50" align="center"><strong>四日留存</strong></td>
                     <td width="50" align="center"><strong>五日留存</strong></td>
                     <td width="50" align="center"><strong>六日留存</strong></td>
                     <td width="50" align="center"><strong>七日留存</strong></td>
                     
                   </tr>""" + d1 + """
                   </table>
                   </div>
                   </div>
                   </div>
        
                    </body>
                    </html>
                    """
        return html
    '''发送邮件'''
    '''其他格式html样式'''
    def data_style(self,df2,html2):
        # d为表格内容
        d2 = ''
        for i in range(len(df2)):
            d2 = d2 + """
            <tr>
                <td>""" + str(df2.index[i]) + """</td>
                <td >""" + str(df2.iloc[i][0]) + """</td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][1]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][2]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][3]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][4]) + """</h2></td>
                <td width="75" align="center"><h2> """ + str(df2.iloc[i][5]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][6]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][7]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][8]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][9]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][10]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][11]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][12]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][13]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df2.iloc[i][14]) + """</h2></td>
                
            </tr>"""

            html = """\
                   <head>
                   <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                   <body>
                   <div id="container">
                   <p><h2>每日基础数据</h2></p>
                   <div id="content">
                    <table width="80%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
                   <tr>
                     <td width="40"><strong>排序</strong></td>
                     <td width="50"><strong>日期</strong></td>
                     <td width="50" align="center"><h3>总设备数</h3></td>
                     <td width="50" align="center"><h3>新注册用户数</h3></td>
                     <td width="50" align="center"><h3>老设备数</h3></td>
                     <td width="50" align="center"><h3>新设备数</h3></td>
                     <td width="50" align="center"><h3>安卓新设备数</h3></td>
                     <td width="50" align="center"><h3>ios新设备数</h3></td>
                     <td width="50" align="center"><h3>启动设备数</h3></td>
                     
                     <td width="50" align="center"><h3>活跃设备数</h3></td>
                     <td width="50" align="center"><h3>安卓活跃设备数</h3></td>
                     <td width="50" align="center"><h3>ios活跃设备数</h3></td>
                     <td width="50" align="center"><h3>每人使用时长(单位/秒)</h3></td>
                     <td width="50" align="center"><h3>每次使用时长(单位/秒)</h3></td>
                     <td width="50" align="center"><h3>总启动次数</h3></td>
                     <td width="50" align="center"><h3>人均启动次数(单位/次)</h3></td>
                   </tr>""" + d2 + """
                   </table>
                   </div>
                   </div>
                   </div>
                   <img src="cid:image1"/>
                   """ + html2 + """
                    </body>
                    </html>
                    """
        return html
    def send(self,html):
        mail_host = "smtp.edspay.com"  # 设置服务器
        mail_user = "guoyiguang@edspay.com"  # 用户名
        mail_pass = "aladin#2018"  # 口令

        sender = 'guoyiguang@edspay.com'
        receivers = ['827267162@qq.com','liquan@edspay.com','xuxiaoming@edspay.com','penghuaying@edspay.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
        # 定义邮件内容
        msgRoot = MIMEMultipart('related')
        # 指定图片为当前目录

        msgRoot['From'] = 'guoyiguang@edspay.com'
        msgRoot['To'] = '827267162@qq.com'

        subject = '用户分析'
        msgRoot['Subject'] = Header(subject, 'utf-8')
        msgAlternative = MIMEMultipart('alternative')
        msgRoot.attach(msgAlternative)
        msgAlternative.attach(MIMEText(html, 'html', 'utf-8'))

        fp1 = open('enable.png', 'rb')
        msgImage1 = MIMEImage(fp1.read())

        fp1.close()

        # 定义图片 ID，在 HTML 文本中引用
        msgImage1.add_header('Content-ID', '<image1>')
        msgRoot.attach(msgImage1)
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, msgRoot.as_string())
        print("邮件发送成功")

        os.remove("enable.png")


        print("examples.png 删除成功")
    '''写入excel'''
    def write(self,df):
        writer = pd.ExcelWriter('out.xlsx')
        df.to_excel(writer,'sheet1')
if __name__ == "__main__":
    m=Operation_mail()
    cur,yesterday= m.getconnect()

    # df = m.remain(cur)
    # html2 = m.remain_style(df)
    df2 = m.data(cur)
    # html = m.data_style(df2, html2)
    # m.send(html)



