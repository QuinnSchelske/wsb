# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 16:14:28 2021

@author: quinn
"""

import praw
import re
import requests
import pymongo
import schedule

from datetime import datetime

reddit = praw.Reddit(client_id='DAn8iSr0GAzVUg', client_secret='-bgl4zBKn0PSN1Nd5FJWlRAjkFLe_g', user_agent='wsb')


client = pymongo.MongoClient("mongodb+srv://qqqdog:WgZhYcQHsLqBIZ7P@cluster0.vb9d7.mongodb.net/db?retryWrites=true&w=majority")
db = client['db']
t_counts=db.ticker_mention_counts


tickerMap = {}
ignoreMap = {'WSB':1, 'DD':1, 'THE':1, 'HOLD':1, 'MOON':1, 'YOLO':1, 'TD':1 , 'NEW':1, 'WHEN':1, 'JUST':1, 'UK':1, 'CEO':1}

def resetTicker():
	tickerMap = {}

def checkTicker(ticker):
    url = "http://d.yimg.com/autoc.finance.yahoo.com/autoc?query={}&region=1&lang=en".format(ticker)
    result = requests.get(url).json()
    if len(result['ResultSet']['Result'])>0 and result['ResultSet']['Result'][0]['symbol'] == ticker:
        return True
    return False

def updateDatabase(t):
    t_counts.find_one_and_update({'Date':datetime.today().strftime('%Y-%m-%d'), 'Ticker':t}, 
                        {"$set": {'Mentions':tickerMap[t]}},upsert=True)
						
schedule.every().day.at("00:00").do(resetTicker)

while True:
	try:
		for comment in reddit.subreddit("wallstreetbets").stream.submissions():
			print(str(datetime.now()) +' '+comment.title)
			tickers = re.findall(r'[A-Z][A-Z]+', str(comment.title))
			tickers = set(tickers)
			print(str(datetime.now())+' '+str(tickers))
			for t in tickers:
				if t in tickerMap:
					tickerMap[t]=tickerMap[t]+1
					updateDatabase(t)
				else:
					if t not in ignoreMap and checkTicker(t):
						tickerMap[t]=1
						updateDatabase(t)
					else:
						ignoreMap[t]=1
	except:
		print("Error ----- restart")
		
    
    

