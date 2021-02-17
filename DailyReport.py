# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 18:46:47 2021

@author: quinn
"""
import pymongo
from datetime import datetime
from datetime import timedelta

import smtplib
import pandas as pd
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

today = datetime.today().strftime('%Y-%m-%d')
today_date = datetime.today()
t_minus_1 = (today_date - timedelta(days=1)).strftime('%Y-%m-%d')
t_minus_3 = (today_date - timedelta(days=3)).strftime('%Y-%m-%d')
t_minus_5 = (today_date - timedelta(days=5)).strftime('%Y-%m-%d')

# creates SMTP session 
email = smtplib.SMTP('smtp.gmail.com', 587) 
  
# TLS for security 
email.starttls() 
  
# authentication
# compiler gives an error for wrong credential. 
email.login("thedrunkwsbtard", "Stonks6969") 


client = pymongo.MongoClient("mongodb+srv://qqqdog:WgZhYcQHsLqBIZ7P@cluster0.vb9d7.mongodb.net/db?retryWrites=true&w=majority")
db = client['db']
t_counts=db.ticker_mention_counts

res = t_counts.aggregate([
    {"$match":{"Date":today}},
    {"$group":{"_id":"$Ticker", 'Mentions':{"$max":"$Mentions"}}},
    {"$project":{"_id":0, "Ticker":"$_id", 'Mentions':1}},
    {'$sort':{"Mentions":-1}},
    {"$limit":15}
    ])

df = pd.DataFrame(list(res))
print(df)

res = t_counts.aggregate([
    {"$match":{"Date":t_minus_1}},
    {"$group":{"_id":"$Ticker", 'Mentions':{"$max":"$Mentions"}}},
    {"$project":{"_id":0, "Ticker":"$_id", 'Mentions':1}},
    {'$sort':{"Mentions":-1}},
    {"$limit":50}
    ])

df_minus_1 = pd.DataFrame(list(res))
print(df_minus_1)



merged_df = df.merge(df_minus_1, on='Ticker', how='left')

merged_df['% Change']=round((merged_df['Mentions_x']/merged_df['Mentions_y']-1)*100,2)

merged_df.columns = ['Mentions Today', 'Ticker', 'Mentions 1D Ago', '% Change']

merged_df = merged_df[['Ticker', 'Mentions Today', 'Mentions 1D Ago', '% Change']]






print(merged_df)

# =============================================================================
# res = t_counts.aggregate([
#     {"$match":{"Date":t_minus_3}},
#     {"$group":{"_id":"$Ticker", 'Mentions':{"$max":"$Mentions"}}},
#     {"$project":{"_id":0, "Ticker":"$_id", 'Mentions':1}},
#     {'$sort':{"Mentions":-1}},
#     {"$limit":20}
#     ])
# 
# df_minus_3 = pd.DataFrame(list(res))
# print(df)
# 
# res = t_counts.aggregate([
#     {"$match":{"Date":t_minus_5}},
#     {"$group":{"_id":"$Ticker", 'Mentions':{"$max":"$Mentions"}}},
#     {"$project":{"_id":0, "Ticker":"$_id", 'Mentions':1}},
#     {'$sort':{"Mentions":-1}},
#     {"$limit":20}
#     ])
# 
# df_minus_5 = pd.DataFrame(list(res))
# print(df)
# 
# =============================================================================

#df.merge(df_minus_1, on="Ticker", how="left")



recipients = ['thedrunkwsbtard@gmail.com', 'georgesitu@yahoo.com', 'nathan.cha00@gmail.com', 'firdaus@gupte.net', 'alyzhang914@gmail.com', 'moolah2994@gmail.com', 'jaeeun0oh@gmail.com', 'bobby.b.chin@gmail.com'] 
emaillist = [elem.strip().split(',') for elem in recipients]
msg = MIMEMultipart()

morning = 'Pre-Market'
midday = 'Mid-Day'
night = 'Post-Market'

prefix=''
if datetime.now().hour<11:
    prefix=morning
elif datetime.now().hour>12:
    prefix=night
else:
    prefix=midday
        

msg['Subject'] = prefix+" WSB Tard Watch "+today
msg['From'] = 'thedrunkwsbtard@gmail.com'


html = """\
<html>
  <head>Beta - not investing advice</head>
  <body>
    {0}
  </body>
</html>
""".format(merged_df.to_html(index=False))

part1 = MIMEText(html, 'html')
msg.attach(part1)

email.sendmail(msg['From'], emaillist , msg.as_string())

email.quit()


