#!/usr/bin/env python3
# -*- coding: utf-8 -*
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import datetime

class Big_dial:
    # 获取连接和日期参数
    def getconnect(self):
        # 数据库ip地址
        host = "192.168.115.105"
        # 数据库名称
        db = "news_manage_test"
        # 登陆用户名
        user = "root"
        # 登陆密码
        password = "Hik12345+"
        # 获取上一天日期
        today = datetime.date.today()
        oneday = datetime.timedelta(days=1)
        yesterday = today - oneday
        # 链接数据库 不指定charset='utf8' 查询出来的中文会出现乱码
        db = pymysql.connect(host, user, password, db, port=3306,charset='utf8')
        # 使用cursor()的方法获取游标
        cur = db.cursor()
        return cur, yesterday

    #查询数据 并返回pandas 格式数据
    def select_data(self,cur,yesterday):
        sql = '''select f.nickname,e.mobile,e.num,e.aspend,e.aaward,e.aprofit,e.bspend,e.baward,e.bprofit,f.gmt_create from ( SELECT 	aa.user_id, aa.mobile, 	aa.num+bb.num as num, aa.spend as aspend, aa.award as aaward, 	aa.profit as aprofit, bb.spend as bspend, bb.award as baward, bb.profit as bprofit FROM 	( SELECT user_id, mobile, num, spend, award, award - spend AS profit FROM ( SELECT user_id, mobile, count(*) AS num, sum(spend_amount) AS spend, sum(award_amount) AS award FROM ( 	SELECT 	* 	FROM 	news_lottery_award_record 	WHERE DATE_FORMAT(gmt_create,'%%Y-%%m-%%d')='%s' AND type = 2 ) a GROUP BY 	user_id, mobile ) b ) aa JOIN ( 	SELECT 		user_id, 		mobile, 		num, 		spend, 		award, 		award - spend AS profit 	FROM 		( 			SELECT 				user_id, 				mobile, 				count(*) AS num, sum(spend_amount) AS spend, sum(award_amount) AS award FROM 	( SELECT 						* 					FROM 						news_lottery_award_record 	WHERE DATE_FORMAT(gmt_create,'%%Y-%%m-%%d')='%s' 	AND type = 1 ) c GROUP BY 	user_id, mobile ) d ) bb ON aa.user_id = bb.user_id)e join (select user_id,substring_index(group_concat(gmt_create order by gmt_create desc),",",1) as gmt_create,substring_index(group_concat(nickname order by gmt_create desc),",",1) as nickname from news_lottery_award_record where DATE_FORMAT(gmt_create,'%%Y-%%m-%%d')='%s' group by user_id)f on e.user_id=f.user_id'''%(yesterday,yesterday,yesterday)
        cur.execute(sql)
        results = cur.fetchall()
        df = pd.DataFrame([ij for ij in i] for i in results)
        # 更改字段名称与数据库列名称对应
        df.rename(columns={0: 'nickname', 1: 'mobile', 2: 'num',3:'aspend',4:'aaward',5:'aprofit',6:'bspend',7:'baward',8:'bprofit',9:'stat_date'}, inplace=True)
        #print(df)
        return df
    # 将数据添加到指定数据库中
    def insert_data(self,df):

        yconnect = create_engine('mysql+pymysql://root:Hik12345+@192.168.115.105:3306/m_eps?charset=utf8')
        # pandas,表名，链接，库名，数据添加模式（追加），不存储索引
        pd.io.sql.to_sql(df,'big_dial_user',yconnect,schema='m_eps',if_exists='append',index=False)


if __name__ == "__main__":
    dial = Big_dial()
    cur,yesterday = dial.getconnect()
    df = dial.select_data(cur,yesterday)
    dial.insert_data(df)