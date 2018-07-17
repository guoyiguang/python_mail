#!/usr/bin/env python3
# -*- coding: utf-8 -*
import smtplib

import os
from email.mime.text import MIMEText
from email.header import Header
import pymysql
from pandas import Series, DataFrame
import time
import pandas as pd
import plotly.plotly as pl
import matplotlib as mpl

mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import datetime


class Mail:
    '''获取链接'''

    def getconnect(self):
        # 数据库ip地址
        host = "10.100.110.72"
        # 数据库名称
        db = "news_manage"
        # 登陆用户名
        user = "root"
        # 登陆密码
        password = "Hik12345+"
        # 获取上一天日期
        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        # 链接数据库
        db = pymysql.connect(host, user, password, db, port=3306)
        # 使用cursor()的方法获取游标
        cur = db.cursor()
        return cur, yesterday,today,db

    '''定义注册用户---未去重'''
    def register_non(self, db):
        sql="select channel,idx_value,stat_date from news_app_analysis where DATE_FORMAT(stat_date,'%%Y-%%m-%%d')='%s' and idx_group='RegisterUser' order by channel"% (yesterday)
        df = pd.read_sql(sql,db)
        print(df)
        '''新建df2 将渠道英文名改成中文'''
        df2 = df['channel'].replace(
            ['meike','sanxing','xiaomi','AppStore', 'Android', 'feipao', 'tencent', 'mumayi', 'huawei', 'lemi1', 'lemi2', 'lemi3',
             'aiqiyi', 'aishangjie', 'anzhi', 'baidu', 'edushi', 'feipao', 'chuizi', 'huakun', 'wzwDSP'],
            ['官方', '三星','小米','苹果', '安卓', '飞跑', '应用宝', '木蚂蚁', '华为', '乐米1', '乐米2', '乐米3', '爱奇艺', '爱上街', '安智', '百度', '门户', '飞跑',
             '锤子', '华坤', '第三方DSP'])

        return  df, df2

    '''定义激活用户'''
	
    def enable(self, db):
        sql = "SELECT channel,idx_value,stat_date FROM `news_app_analysis` WHERE DATE_FORMAT(stat_date,'%%Y-%%m-%%d')='%s' and idx_group='channel' AND idx = 'day' order by channel"%yesterday
        df = pd.read_sql(sql,db)
        '''新建df2 将渠道英文名改成中文'''
        df2 = df['channel'].replace(
            ['meike','sanxing','xiaomi', 'AppStore', 'Android', 'feipao', 'tencent', 'mumayi', 'huawei', 'lemi1', 'lemi2', 'lemi3',
             'aiqiyi', 'aishangjie', 'anzhi', 'baidu', 'edushi', 'feipao', 'chuizi', 'huakun', 'wzwDSP'],
            ['官方', '三星', '小米','苹果', '安卓', '飞跑', '应用宝', '木蚂蚁', '华为', '乐米1', '乐米2', '乐米3', '爱奇艺', '爱上街', '安智', '百度', '门户', '飞跑',
             '锤子', '华坤', '第三方DSP'])

        return  df, df2

    '''定义活跃用户'''

    def active(self, db):
        sql = "select channel,idx_value,stat_date from news_app_analysis WHERE DATE_FORMAT(stat_date,'%%Y-%%m-%%d')='%s' and idx_group='ActiveUsers' order by channel"%yesterday
        df = pd.read_sql_query(sql,db)
        '''新建df2 将渠道英文名改成中文'''
        df2 = df['channel'].replace(
            ['meike','sanxing','xiaomi','AppStore', 'Android', 'feipao', 'tencent', 'mumayi', 'huawei', 'lemi1', 'lemi2', 'lemi3',
             'aiqiyi', 'aishangjie', 'anzhi', 'baidu', 'edushi', 'feipao', 'chuizi', 'huakun', 'wzwDSP'],
            ['官方', '三星','小米','苹果', '安卓', '飞跑', '应用宝', '木蚂蚁', '华为', '乐米1', '乐米2', '乐米3', '爱奇艺', '爱上街', '安智', '百度', '门户', '飞跑',
             '锤子', '华坤', '第三方DSP'])
        return df, df2

    '''定义留存'''

    def remain(self, db):
        sql = "SELECT idx,idx_value,channel,stat_date  FROM `news_app_analysis` WHERE  DATE_FORMAT(gmt_create,'%%Y-%%m-%%d')='%s' and idx_group='user_save' and idx like '%%day%%' order by stat_date"%today
        df = pd.read_sql(sql,db)
        '''新建df2 将渠道英文名改成中文'''
        df2 = df['channel'].replace(
            ['meike','sanxing','xiaomi','all', 'AppStore', 'Android', 'feipao', 'tencent', 'mumayi', 'huawei', 'lemi1', 'lemi2', 'lemi3',
             'aiqiyi', 'aishangjie', 'anzhi', 'baidu', 'edushi', 'feipao', 'chuizi', 'huakun', 'wzwDSP'],
            ['官方', '三星','小米','整体', '苹果', '安卓', '飞跑', '应用宝', '木蚂蚁', '华为', '乐米1', '乐米2', '乐米3', '爱奇艺', '爱上街', '安智', '百度', '门户',
             '飞跑',
             '锤子', '华坤', '第三方DSP'])
        print(df['idx'])
        df3 = df['idx'].replace(
            ['1_day_save','2_day_save','3_day_save','4_day_save','5_day_save','6_day_save','7_day_save','15_day_save',],
            ['一日留存','二日留存','三日留存','四日留存','五日留存','六日留存','七日留存','十五日留存']
        )
        return df,df2,df3

    '''制作html页面并发送邮件'''

    def send(self, df1_non,df1_1_non, df2, df2_2, df3, df3_3, df4, df4_4,df4_4_4):
        # d为表格内容
        d1_non = ''
        for i in range(len(df1_non)):
            d1_non = d1_non + """
            <tr>
                <td>""" + str(df1_non.index[i]) + """</td>
                <td>""" + str(df1_1_non.iloc[i]) + """</td>
                <td width="60" align="center">""" + str(df1_non.iloc[i][1]) + """</td>
                <td width="75">""" + str(df1_non.iloc[i][2]) + """</td>
            </tr>"""

        d2 = ''
        for i in range(len(df2)):
            d2 = d2 + """
            <tr>
                <td>""" + str(df2.index[i]) + """</td>
                <td>""" + str(df2_2.iloc[i]) + """</td>
                <td width="60" align="center">""" + str(df2.iloc[i][1]) + """</td>
                <td width="75">""" + str(df2.iloc[i][2]) + """</td>
            </tr>"""

        d3 = ''
        for i in range(len(df3)):
            d3 = d3 + """
                    <tr>
                        <td>""" + str(df3.index[i]) + """</td>
                        <td>""" + str(df3_3.iloc[i]) + """</td>
                        <td width="60" align="center">""" + str(df3.iloc[i][1]) + """</td>
                        <td width="75">""" + str(df3.iloc[i][2]) + """</td>
            </tr>"""

        d4 = ''
        for i in range(len(df4)):
            d4 = d4 + """
                            <tr>
                                <td>""" + str(df4.index[i]) + """</td>
                                <td>""" + str(df4_4_4.iloc[i]) + """</td>
                                <td width="60" align="center">""" + str(df4.iloc[i][1]) + """</td>
                                <td width="75">""" + str(df4_4.iloc[i]) + """</td>
                                <td width="75">""" + str(df4.iloc[i][3]) + """</td>
                    </tr>"""

        # web页面内容
        html = """\
        <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        <body>
        <div id="container">
        <p><h1>新增注册用户_未去重</h1></p>
        <div id="content">
         <table width="55%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
        <tr>
          <td width="40"><strong>排序</strong></td>
          <td width="50"><strong>渠道名称</strong></td>
          <td width="60" align="center"><strong>注册数目</strong></td>
          <td width="50"><strong>日期</strong></td>
        </tr>""" + d1_non + """
        </table>
        </div>
        </div>
        </div>
				
		
        <div id="container">
        <p><h1>新增激活用户</h1></p>
        <div id="content">
         <table width="55%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
        <tr>
          <td width="40"><strong>排序</strong></td>
          <td width="50"><strong>渠道名称</strong></td>
          <td width="60" align="center"><strong>激活数目</strong></td>
          <td width="50"><strong>日期</strong></td>
        </tr>""" + d2 + """
        </table>
        </div>
        </div>
        </div>
        


        <div id="container">
        <p><h1>活跃用户各个渠道分析</h1></p>
        <div id="content">
         <table width="55%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
        <tr>
          <td width="40"><strong>排序</strong></td>
          <td width="50"><strong>渠道名称</strong></td>
          <td width="60" align="center"><strong>活跃数目</strong></td>
          <td width="50"><strong>日期</strong></td>
        </tr>""" + d3 + """
        </table>
        </div>
        </div>
        </div>
      

        <div id="container">
        <p><h1>展现每日留存数据</h1></p>
        <div id="content">
         <table width="55%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
        <tr>
          <td width="40"><strong>排序</strong></td>
          <td width="50"><strong>留存时间</strong></td>
          <td width="50"><strong>留存数目</strong></td>
          <td width="60" align="center"><strong>渠道名称</strong></td>
          <td width="50"><strong>日期</strong></td>
        </tr>""" + d4 + """
        </table>
        </div>
        </div>
        </div>


        </body>
        </html>
            """
        # # 第三方 SMTP 服务
        mail_host = "smtp.edspay.com"  # 设置服务器
        mail_user = "guoyiguang@edspay.com"  # 用户名
        mail_pass = "aladin#2018"  # 口令

        sender = 'guoyiguang@edspay.com'
        # 添加接收人
        receivers = ['827267162@qq.com',
                     ]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
        # 定义邮件内容
        msgRoot = MIMEMultipart('related')
        # 指定图片为当前目录

        msgRoot['From'] = 'guoyiguang@edspay.com'
        msgRoot['To'] = '827267162@qq.com'
        subject = '爱上头条分渠道用户分析'
        msgRoot['Subject'] = Header(subject, 'utf-8')
        msgAlternative = MIMEMultipart('alternative')
        msgRoot.attach(msgAlternative)
        msgAlternative.attach(MIMEText(html, 'html', 'utf-8'))
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, msgRoot.as_string())
        print("邮件发送成功")

if __name__ == "__main__":
    m = Mail()
    # 提供链接
    cur,yesterday,today,db = m.getconnect()
    # 未去重注册
    register_df_non, register_df_2_non = m.register_non(db)
    # 激活
    enable_df, enable_df_2 = m.enable(db)
    # 活跃
    active_df, active_df_2 = m.active(db)
    # 留存
    remain_df, remain_df_2,remain_df_3 = m.remain(db)
    # 发送邮件
    m.send(register_df_non, register_df_2_non,enable_df, enable_df_2, active_df, active_df_2, remain_df, remain_df_2,remain_df_3)
