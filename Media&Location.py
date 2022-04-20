#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import pandas as pd
import os


# In[23]:


text1=[]
id1=[]
id2=[]
created1=[]
media_key1=[]
media_type1=[]
loc_key1=[]
loc_geo1=[]
months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']


# In[24]:


for k in range (2014, 2023):
#     print('year  ' + str(k))
    for j in range (1,13):
#         print("month  " + str(j))
        for i in range(1,31):
            if os.path.isfile('./Australia Wildfires/' + str(k) + str(months[j-1]) + str(i+1)+ '.json') == True:
                with open('./Australia Wildfires/' + str(k) + str(months[j-1]) + str(i+1)+ '.json') as f:
                    data = json.load(f)
#                     print(data)
#                     print('day  ' + str(i))
                    if  data['meta']['result_count'] == 0:
                        continue 
                    else:
                        for l in range(len(data["data"])):
                            id1.append(data['data'][l]['id'])
                            text1.append(data['data'][l]['text'])
                            created1.append(data['data'][l]['created_at'])

                    
                        try:
                            for l in range(len(data['includes']['media'])):
                                #print(l)
                                id2.append(data['data'][l]['id'])
                                media_key1.append(data['includes']['media'][l]['media_key'])
                                media_type1.append(data['includes']['media'][l]['type'])
                        except KeyError:
                            continue
                            
                        try:
                            for h in range(len(data['includes']['places'])):
                                #print(h)
                                loc_key1.append(data['includes']['places'][h]['id'])
                                loc_geo1.append(data['includes']['places'][h]['geo'])
                                media_key1.append(data['includes']['media'][h]['media_key'])
                        except KeyError:
                            continue
            else:
                continue
                #print('1')
                


# In[25]:


text_dt= pd.DataFrame(text1, columns=['text'])

id_dt= pd.DataFrame(id1, columns=['id'])

created1_dt= pd.DataFrame(created1, columns=['created_at'])

loc_key = pd.DataFrame(loc_key1, columns=['loc_key'])

dfComb = pd.concat([id_dt.reset_index(drop=True), text_dt, created1_dt,loc_key], axis=1)


# In[26]:


id_df = pd.DataFrame(id2, columns=['id'])
media_key = pd.DataFrame(media_key1, columns=['media_key'])
media_type = pd.DataFrame(media_type1, columns = ['media_type'])

media = pd.concat([id_df.reset_index(drop=True), media_key, media_type], axis=1)
media


# In[27]:


dfComb


# In[28]:


loc_media = pd.merge(dfComb, media, on='id')
loc_media


# In[59]:


get_ipython().system('jupyter notebook --NotebookApp.iopub_data_rate_limit=1.0e10')


# In[31]:


dfComb.to_csv('Australia_Wildfire_dfComb.csv', index=False, header=True)
media.to_csv('Australia_Wildfire_media.csv', index=False, header=True)
loc_media.to_csv('Australia_Wildfire_loc_media.csv', index=False, header=True)


# In[ ]:




