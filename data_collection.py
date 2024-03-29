# -*- coding: utf-8 -*-

import requests
import os
import json

import pandas as pd
import csv
import datetime
import dateutil.parser
import unicodedata

import time

from google.colab import drive
drive.mount('/content/drive')
os.chdir('/content/drive/MyDrive/2022/geo-tag/Hurricane_Irma')
os.getcwd()

os.environ['TOKEN'] = 'BEARER TOKEN'

def auth():
  return os.getenv('TOKEN')

def create_headers(bearer_token):
  headers = {'Authorization': 'Bearer {}'.format(bearer_token)}
  return headers

def create_url(keyword, start_date, end_date, max_results = 10):
  search_url = "https://api.twitter.com/2/tweets/search/all" #Change to the endpoint you want to collect data from

  #change params based on the endpoint you are using
  query_params = {'query': keyword,
                  'start_time': start_date,
                  'end_time': end_date,
                  'max_results': max_results,
                  'expansions': 'author_id,in_reply_to_user_id,geo.place_id,attachments.media_keys',
                  'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                  'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                  'media.fields': 'type',
                  'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                  'next_token': {}}
  return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

for k in range (2017, 2018): #Year

    for j in range (9,10): #month ex. (3,6)
        
        for i in range(1,31): #day
            print("day" + str(i))
            print("month" + str(j))
            print(k)
            bearer_token = auth()
            headers = create_headers(bearer_token)
            keyword = "lang:EN (hurricane irma) OR (irma storm) OR (storm irma) OR (irma hurricane) OR (irma)"

            start_time = str(k)+"-"+ str(j) +"-"+str(i)+"T00:00:00.000Z"  
            if j==2:
                end_time = str(k)+"-"+ str(j) +"-"+str(i+1)+"T00:00:00.001Z"
                if k !=2020 and i== 28 or i==29 or i== 30: #this is for leap years and needs to be adjusted
                    break
                
            else:
                if j==1 or j==3 or j==5 or j==7 or j==8 or j==10 or j==12:    
                    end_time = str(k)+"-"+ str(j) +"-"+str(i+1)+"T00:00:00.001Z"
                
                else:
                    end_time = str(k)+"-"+ str(j) +"-"+str(i)+"T00:00:00.001Z"
                
            max_results = 500
                       
            url = create_url(keyword, start_time,end_time, max_results)
            
            json_response = connect_to_endpoint(url[0], headers, url[1])
            
            months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
            
            if j==1 or j==3 or j==5 or j==7 or j==8 or j==10 or j==12:  
                with open('Data_Directory' +str(k) + str(months[j-1]) + str(i+1) + '.json', 'w') as f:
                    json.dump(json_response, f)
            else:
                
                with open('Data_Directory' + str(k) + str(months[j-1]) + str(i) + '.json', 'w') as f:
                    json.dump(json_response, f)
                
            time.sleep(2.7) #this is a time delay so we don't pass Twitter's limit.

