# -*- coding: utf-8 -*-

import tweepy
import pandas as pd
import time
import json
import pandas as pd
import os

from google.colab import drive
drive.mount('/content/drive')
os.chdir('/content/drive/MyDrive/2022/geo-tag/')
os.getcwd()

id = []
media_key = []
created_at = []
text = []
media_type = []
place_id = []

months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

author_id = []

for k in range (2014, 2023):
    #print('year  ' + str(k))
    for j in range (1,13):
        # print("month  " + str(j))
        for i in range(1,32):
            if os.path.isfile('Ecuador_Earthquake/' + 'Data_Directory' + str(k) + str(months[j-1]) + str(i+1)+ '.json') == True:
                with open('Ecuador_Earthquake/' + 'Data_Directory' + str(k) + str(months[j-1]) + str(i+1)+ '.json') as f:
                    data = json.load(f)
                    # print('day  ' + str(i+1))
                    # print(data['meta'])
                    if  data['meta']['result_count'] == 0:
                        continue 
                    else:
                       print('year' + str(k) + 'month' + str(months[j-1]) + 'day' + str(i+1))
                       for m in range(len(data['data'])):
                          try:

                            data['data'][m]['attachments']
                            data['data'][m]['geo']
                            # daily_media_key = []

                            for n in range(len(data['data'][m]['attachments']['media_keys'])):
                              place_id.append(data['data'][m]['geo']['place_id'])
                              id.append(data['data'][m]['id'])
                              created_at.append(data['data'][m]['created_at'])
                              text.append(data['data'][m]['text'])
                              media_key.append(data['data'][m]['attachments']['media_keys'][n])
                              # daily_media_key.append(data['data'][m]['attachments']['media_keys'][n])

                              for q in range(len(data['includes']['media'])):
                                  if data['data'][m]['attachments']['media_keys'][n] == data['includes']['media'][q]['media_key']:
                                    media_type.append(data['includes']['media'][q]['type'])

                          except KeyError:
                            pass

                          # try:
                          #    data['includes']['media']
                          #    for a in range(len(data['includes']['media'])):
                          #      for b in range(len(daily_media_key)):
                          #        if daily_media_key[b] == data['includes']['media'][a]['media_key']:
                          #         media_type.append(data['includes']['media'][a]['type'])
                          # # for a in range(len(media_key)):
                          # #    for b in range(len(data['includes']['media'])):
                          # #      if media_key[a] == data['includes']['media'][b]['media_key']:
                          # #        media_type.append(data['includes']['media'][b]['type'])
                          # except KeyError:
                          #   pass

            else:
                continue
                #print('1')
                
                
text_dt= pd.DataFrame(text, columns=['text'])
id_dt= pd.DataFrame(id, columns=['id'])
created1_dt= pd.DataFrame(created_at, columns=['created_at'])
media_key= pd.DataFrame(media_key, columns=['media_key'])
media_type= pd.DataFrame(media_type, columns=['media_type'])
geo_key = pd.DataFrame(place_id, columns=['geo_key'])
dfComb = pd.concat([id_dt.reset_index(drop=True), text_dt, created1_dt,media_key,media_type, geo_key], axis=1)
print(len(dfComb.id.unique()))
dfComb

# dfComb.id.duplicated().sum()

#dfComb.loc_key.duplicated().sum()

#dfComb.media_key.duplicated().sum()


#dfComb.to_csv('D:\\Penn State SP2022\\Benchmarking Twitter Study\\Third Dataset\\Location Data\\Virginia Beach Shooting.csv', index=False)         #Can be uncommented Safely

                            
#%%

# data['data'] # length = 480
media_key = []
place_id = []
id = []
created_at = []
place_id = []
text = []
media_type = []
for i in range(len(data['data'])):
  try:
    data['data'][i]['attachments']
    # data['data'][i]['geo']
    for l in range(len(data['data'][i]['attachments']['media_keys'])):
      media_key.append(data['data'][i]['attachments']['media_keys'][l])
      # place_id.append(data['data'][i]['geo']['place_id'])
      id.append(data['data'][i]['id'])
      created_at.append(data['data'][i]['created_at'])
      text.append(data['data'][i]['text'])
  except KeyError:
    pass

len(media_key) #85
# unique_media_key = []
# for m in range(len(media_key)):
#   if media_key[m] not in unique_media_key:
#     unique_media_key.append(media_key[m])
# len(unique_media_key) # 35

# data['includes']['media'] # length = 35
# data['includes']['places'] #length = 5
# data['includes']['users'] #length = 504
# id2=[]
# for i in range(len(data['includes']['users'])):
#   id = data['includes']['users'][i]['id']
#   id2.append(id)
media_type = []
for i in range(len(data['includes']['media'])):
  for n in range(len(media_key)):
    if data['includes']['media'][i]['media_key'] == media_key[n]:
      media_type.append(data['includes']['media'][i]['type'])

dfComb.media_type.unique() #array(['photo', 'video', 'animated_gif'], dtype=object)
df_photo = dfComb[dfComb['media_type'] == 'photo']
df_video = dfComb[dfComb['media_type'] == 'video']
df_gif = dfComb[dfComb['media_type'] == 'animated_gif']

print(len(df_photo.id.unique()))
print(df_photo.shape)

print(len(df_video.id.unique()))
print(df_video.shape)

print(len(df_gif.id.unique()))
print(df_gif.shape)

"""# Location + Media WORKS!!
* Srilanka_Flood : 7 people posted 12 photos 
* Kaikoura Earthquake : photo 5 
* Italy Earthquake : None
* Hurricane_Irma : None
* Ecuador_Earthquake : 117 people posted 154 media posts
                        114 people posted 151 photos
                        2 people posted 2 videos
                        1 person posted 1 gif

"""

