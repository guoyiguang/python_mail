#!/usr/bin/env python3
# -*- coding: utf-8 -*
import smtplib

from email.mime.text import MIMEText
from email.header import Header
import pymysql
import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
from email.mime.multipart import MIMEMultipart




class Mail_total:
    '''获取链接'''

    def getconnect(self):
        # 数据库ip地址
        host = "10.100.110.64"
        # 数据库名称
        db = "news_manage"
        # 登陆用户名
        user = "root"
        # 登陆密码
        password = "Hik12345+"
        # 链接数据库
        db = pymysql.connect(host, user, password, db, port=3306)
        # 使用cursor()的方法获取游标
        cur = db.cursor()
        return cur
    '''查询数据'''
    def select_data(self,cur):
        sql = '''SELECT aaa.stat_date,aaa.channel,bbb.idx_value "aa",ccc.idx_value "bb",ROUND(ccc.idx_value/bbb.idx_value,4) "cc",  ddd.idx_value "dd", ROUND((aaa.`1`/bbb.idx_value),4) "11", ROUND((aaa.`2`/bbb.idx_value),4)  "22", ROUND((aaa.`3`/bbb.idx_value),4)  "33", ROUND((aaa.`4`/bbb.idx_value),4)  "44", ROUND((aaa.`5`/bbb.idx_value),4)  "55", ROUND((aaa.`6`/bbb.idx_value),4)  "66", ROUND((aaa.`7`/bbb.idx_value),4)  "77" from (select stat_date,channel,sum(1_all) "1",sum(2_all) "2",sum(3_all) "3",sum(4_all) "4",sum(5_all) "5", sum(6_all) "6",sum(7_all) "7" from ( SELECT b.stat_date,channel, case idx when "1_day_save" then idx_value end "1_all", case idx when "2_day_save" then idx_value end "2_all", case idx when "3_day_save" then idx_value end "3_all", case idx when "4_day_save" then idx_value end "4_all", case idx when "5_day_save" then idx_value end "5_all", case idx when "6_day_save" then idx_value end "6_all", case idx when "7_day_save" then idx_value end "7_all" FROM (select stat_date,idx,idx_value,channel from news_app_analysis WHERE idx_group = "user_save_channel" GROUP BY idx,channel,stat_date,idx_value  )b  ) a where a.stat_date > date_sub(CURRENT_DATE,INTERVAL 9 DAY) GROUP BY stat_date,channel ORDER BY channel,stat_date )aaa LEFT JOIN (SELECT stat_date,channel,idx_value FROM `news_app_analysis` WHERE idx_group like "channel" and stat_date > date_sub(CURRENT_DATE,INTERVAL 9 DAY) order by gmt_create desc)bbb on aaa.stat_date=bbb.stat_date and aaa.channel=bbb.channel  LEFT JOIN ( SELECT stat_date,channel,idx_value FROM `news_app_analysis` WHERE idx_group = "RegisterDistinctUser" and stat_date > date_sub(CURRENT_DATE,INTERVAL 9 DAY) order by gmt_create desc )ccc on aaa.stat_date=ccc.stat_date and aaa.channel=ccc.channel  LEFT JOIN ( SELECT stat_date,channel,idx_value FROM `news_app_analysis` WHERE idx_group = "ActiveUsers" and stat_date > date_sub(CURRENT_DATE,INTERVAL 9 DAY) order by gmt_create desc )ddd on aaa.stat_date = ddd.stat_date and aaa.channel = ddd.channel'''
        cur.execute(sql)
        results = cur.fetchall()
        df = pd.DataFrame([ij for ij in i] for i in results)
        df1 = df.replace([None], [' '])

        df2 = df[1].replace(
            ['meike','alibaba','sanxing', 'xiaomi', 'AppStore', 'Android', 'feipao', 'tencent', 'mumayi', 'huawei', 'lemi1',
             'lemi2', 'lemi3',
             'aiqiyi', 'aishangjie', 'anzhi', 'baidu', 'edushi', 'feipao', 'chuizi', 'huakun', 'wzwDSP'],
            ['官方','阿里巴巴','三星', '小米', '苹果', '安卓', '飞跑', '应用宝', '木蚂蚁', '华为', '乐米1', '乐米2', '乐米3', '爱奇艺', '爱上街', '安智', '百度',
             '锤子', '华坤', '第三方DSP'])
        return df1,df2
    '''制作html页面'''
    def make_html(self,df1,df2):
        d = ''

        for i in range(len(df1)):
            d = d+"""
            <tr>
                <td>""" + str(df1.index[i]) + """</td>
                <td >""" + str(df1.iloc[i][0]) + """</td>               
                <td width="75"align="center"><h2>""" + str(df2.iloc[i]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][2]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][3]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][4]) + """</h2></td>
                <td width="75" align="center"><h2> """ + str(df1.iloc[i][5]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][6]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][7]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][8]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][9]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][10]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][11]) + """</h2></td>
                <td width="75"align="center"><h2>""" + str(df1.iloc[i][12]) + """</h2></td>
            </tr>
            """
        html = """\
                               <head>
                               <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
                               <body>
                               <div id="container">
                               <p><h2>分渠道展示激活、注册、注册率、活跃和七日留存数据</h2></p>
                               <div id="content">
                                <table width="70%" border="2" bordercolor="black" cellspacing="0" cellpadding="0">
                               <tr>
                                 <td width="40"><strong>排序</strong></td>
                                 <td width="50"><strong>日期</strong></td>
                                  <td width="50"><strong>渠道</strong></td>  
                                  <td width="50"><strong>激活</strong></td>
                                  <td width="50"><strong>注册</strong></td>
                                  <td width="50"><strong>注册率</strong></td>
                                  <td width="50"><strong>活跃</strong></td>                
                                 <td width="50" align="center"><strong>次日留存</strong></td>
                                 <td width="50" align="center"><strong>二日留存</strong></td>
                                 <td width="50" align="center"><strong>三日留存</strong></td>
                                 <td width="50" align="center"><strong>四日留存</strong></td>
                                 <td width="50" align="center"><strong>五日留存</strong></td>
                                 <td width="50" align="center"><strong>六日留存</strong></td>
                                 <td width="50" align="center"><strong>七日留存</strong></td>

                               </tr>""" + d + """
                               </table>
                               </div>
                               </div>
                               </div>

                                </body>
                                </html>
                                """
        return html
    def send(self,html):
        mail_host = "smtp.edspay.com"  # 设置服务器
        mail_user = "guoyiguang@edspay.com"  # 用户名
        mail_pass = "aladin#2018"  # 口令

        sender = 'guoyiguang@edspay.com'
        # 添加接收人
        receivers = ['827267162@qq.com',
                     'liquan@edspay.com',
                     'guwuyan@edspay.com',
                     'jinxiaofeng@edspay.com',
                     'shenjincheng@edspay.com',
                     'yebaicheng@edspay.com',
                     'wenjiezhen@edspay.com',
                     'xingzhongcheng@edspay.com',
                     'zhumingmin@edspay.com',
                     'changhuajie@edspay.com',
                     'huruixia@edspay.com'
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
        # 定义图片 ID，在 HTML 文本中引用
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, msgRoot.as_string())
        print("邮件发送成功")
if __name__ == "__main__":
    m = Mail_total()
    cur = m.getconnect()
    df1,df2 = m.select_data(cur)
    html = m.make_html(df1,df2)
    m.send(html)