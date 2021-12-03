#!/usr/bin/env python
# coding: utf-8

# In[41]:


import psycopg2
import datetime
import time
import math
from datetime import datetime
import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 3000)
pd.set_option('display.max_seq_items', 3000)
pd.set_option('max_colwidth', 2000)

pd.options.mode.chained_assignment = None  # default='warn'

# dr.Client(token='M2_MgM0U0bUR-x4w3k1WuCkOAbEuWysS', endpoint='https://app.datarobot.com/api/v2')

def query_redshift(sql):
   conn = psycopg2.connect(
       database='dev', user='awsuser',
       host="redshift-cluster-1.csvogyduuzab.us-east-2.redshift.amazonaws.com", port=5439, password="Pass1234")
   time_start = datetime.datetime.now()
   cur = conn.cursor()
   cur.execute(sql)
   results_query = cur.fetchall()
   results_colnames = [desc[0] for desc in cur.description]
   cur.close()
   conn.close()
   time_passed = datetime.datetime.now() - time_start
   print('Seconds taken: %s' % time_passed.total_seconds())
   return pd.DataFrame(results_query, columns=results_colnames)

AWS_KEY = "AKIAW5TFIVP5MY3DD44R"
AWS_SECRET = "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"
REGION_NAME = 'us-east-2'

# Dallin Binance API/Secret
dallin_binance_key = "TVD1AV0EfD1fVXwxpSYcjo4svuGMbTnBRd8VhyhEz3wam1wPJBAf4fEGBndMxuCt"
dallin_binance_secret = "zNPDZrZjH7ZUZHjHIuCEmZ0kLNaK2pEpRsVA1JBokPIipFq7wPOsRrrqtcJgbcje"

ryan_binance_key = "TVD1AV0EfD1fVXwxpSYcjo4svuGMbTnBRd8VhyhEz3wam1wPJBAf4fEGBndMxuCt"
ryan_binance_secret = "Je2K9BSxkn7fVwMdd03aFvsRFBTxiHbKizLuwGLQPTNVommT914tI3TSpV2B8Z07"

import boto3
import requests
import pandas as pd
import numpy as np
import datetime
import time
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from binance.client import Client
from sqlalchemy import create_engine

client = Client(dallin_binance_key,dallin_binance_secret,tld='us')
session = boto3.Session(aws_access_key_id = AWS_KEY,aws_secret_access_key = AWS_SECRET,region_name = REGION_NAME)
s3 = session.resource('s3')

info = []
intervals = 10
time_res = []
for i in range(intervals):
    created_at = datetime.datetime.now()
    time_res = client.get_server_time()
    timestamp = (datetime.datetime(1970,1,1,0,0,0) 
         + datetime.timedelta(milliseconds=time_res['serverTime'])).strftime("%Y-%d-%m %H:%M:%S")
    info = client.get_all_tickers()
    
    # s3 push
    object = s3.Object('binance-macro-economy','s3_obj_{}.json'.format(int(time_res['serverTime']/1000)))
    info_bytes = bytes(str(info),'utf-8')
    result = object.put(Body=info_bytes)
    
    # redshift push
    df1 = pd.DataFrame({'created_at': {0: created_at}})
    df1['key'] = 0
    
    df2 = pd.DataFrame(info).T.rename(columns={
    0: 'BTCUSD',1: 'ETHUSD',2: 'XRPUSD',3: 'BCHUSD',4: 'LTCUSD',5: 'USDTUSD',6: 'BTCUSDT',7: 'ETHUSDT',8: 'XRPUSDT',
    9: 'BCHUSDT',10: 'LTCUSDT',11: 'BNBUSD',12: 'BNBUSDT',13: 'ETHBTC',14: 'XRPBTC',15: 'BNBBTC',16: 'LTCBTC',17: 'BCHBTC',
    18: 'ADAUSD',19: 'BATUSD',20: 'ETCUSD',21: 'XLMUSD',22: 'ZRXUSD',23: 'ADAUSDT',24: 'BATUSDT',25: 'ETCUSDT',26: 'XLMUSDT',
    27: 'ZRXUSDT',28: 'LINKUSD',29: 'RVNUSD',30: 'DASHUSD',31: 'ZECUSD',32: 'ALGOUSD',33: 'IOTAUSD',34: 'BUSDUSD',35: 'BTCBUSD',
    36: 'DOGEUSDT',37: 'WAVESUSD',38: 'ATOMUSDT',39: 'ATOMUSD',40: 'NEOUSDT',41: 'NEOUSD',42: 'VETUSDT',43: 'QTUMUSDT',
    44: 'QTUMUSD',45: 'NANOUSD',46: 'ICXUSD',47: 'ENJUSD',48: 'ONTUSD',49: 'ONTUSDT',50: 'ZILUSD',51: 'ZILBUSD',
    52: 'VETUSD',53: 'BNBBUSD',54: 'XRPBUSD',55: 'ETHBUSD',56: 'ALGOBUSD',57: 'XTZUSD',58: 'XTZBUSD',59: 'HBARUSD',60: 'HBARBUSD',
    61: 'OMGUSD',62: 'OMGBUSD',63: 'MATICUSD',64: 'MATICBUSD',65: 'XTZBTC',66: 'ADABTC',67: 'REPBUSD',68: 'REPUSD',69: 'EOSBUSD',
    70: 'EOSUSD',71: 'DOGEUSD',72: 'KNCUSD',73: 'KNCUSDT',74: 'VTHOUSDT',75: 'VTHOUSD',76: 'USDCUSD',77: 'COMPUSDT',78: 'COMPUSD',
    79: 'MANAUSD',80: 'HNTUSD',81: 'HNTUSDT',82: 'MKRUSD',83: 'MKRUSDT',84: 'DAIUSD',85: 'ONEUSDT',86: 'ONEUSD',87: 'BANDUSDT',
    88: 'BANDUSD',89: 'STORJUSDT',90: 'STORJUSD',91: 'BUSDUSDT',92: 'UNIUSD',93: 'UNIUSDT',94: 'SOLUSD',95: 'SOLUSDT',
    96: 'LINKBTC',97: 'VETBTC',98: 'UNIBTC',99: 'EGLDUSDT',100: 'EGLDUSD',101: 'PAXGUSDT',102: 'PAXGUSD',103: 'OXTUSDT',
    104: 'OXTUSD',105: 'ZENUSDT',106: 'ZENUSD',107: 'BTCUSDC',108: 'ONEBUSD',109: 'FILUSDT',110: 'FILUSD',111: 'AAVEUSDT',
    112: 'AAVEUSD',113: 'GRTUSD',114: 'SUSHIUSD',115: 'ANKRUSD',116: 'AMPUSD',117: 'SHIBUSDT',118: 'SHIBBUSD',119: 'CRVUSDT',
    120: 'CRVUSD',121: 'AXSUSDT',122: 'AXSUSD',123: 'SOLBTC',124: 'AVAXUSDT',125: 'AVAXUSD',126: 'CTSIUSDT',127: 'CTSIUSD',
    128: 'DOTUSDT', 129: 'DOTUSD', 130: 'YFIUSDT', 131: 'YFIUSD'}
    ).reset_index().tail(1).drop('index', axis = 1)
    df2['key'] = 0
    
    df = df1.merge(df2, on = 'key', how = 'inner').drop('key', axis = 1)

    conn = create_engine('postgresql://awsuser:Pass1234@redshift-cluster-1.csvogyduuzab.us-east-2.redshift.amazonaws.com:5439/dev')

    df.to_sql('binance', conn, index=False, if_exists='append')


# In[ ]:





# In[ ]:





# In[ ]:





# In[88]:





# In[92]:





# In[ ]:





# In[43]:





# In[ ]:





# In[350]:





# In[347]:





# In[ ]:





# In[ ]:





# In[208]:





# In[135]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[41]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[135]:





# In[ ]:





# In[99]:





# In[ ]:





# In[125]:





# In[137]:





# In[ ]:




