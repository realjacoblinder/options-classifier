import pandas as pd
import yahoo_fin.stock_info as si
import dateutil.parser as dp
import os
from tqdm import tqdm
import datetime as dt
from collections import defaultdict
import time
import json

#pd.set_option('mode.chained_assignment', None)

def get_band(ticker, date, band_age = 20):
    start_date = date - dt.timedelta(days=band_age)
    try:
        data = si.get_data(ticker, start_date=start_date, end_date=date)
    except KeyError:
        time.sleep(2)
        try:
            data = si.get_data(ticker, start_date=start_date, end_date=date)
        except KeyError:
            return -1
    
    stockprices = data.drop(columns=['open', 'high', 'low', 'close', 'volume', 'ticker'])
    if len(data) < band_age:
        band_age = len(data)
    stockprices['MA'] = stockprices['adjclose'].rolling(window=band_age).mean()
    stockprices['STD'] = stockprices['adjclose'].rolling(window=band_age).std() 
    stockprices['Upper'] = stockprices['MA'] + (stockprices['STD'] * 2)
    stockprices['Lower'] = stockprices['MA'] - (stockprices['STD'] * 2)

    stockprices = stockprices.dropna()
    return stockprices


def get_expiry_from_name(c_name, ticker):
    offset = len(ticker)
    expiry = c_name[offset:offset+3] + c_name[offset+3:offset+5] + c_name[offset+5:offset+6]
    expiry = expiry[2:] + expiry[0:2]
    return dp.parse(expiry)

def is_assigned(row):
    if row['adjclose at expiry'] == "UNKNOWN":
        return "UNKNOWN"
    if row['Contract'] == 'P':
        if  row['adjclose at expiry'] < row['Strike']:
            return 1
        else:
            return 0
    else:
        if row['adjclose at expiry'] > row['Strike']:
            return 0
        else:
            return 1

def active_contracts(row):
    last = row['Last Trade Date']
    pull = row['Pull Timestamp']
    if last.day == pull.day & last.month == pull.month & last.year == pull.year:
        return 1
    else:
        return 0


expiration_dict = defaultdict(lambda: {})
def get_close_price(ticker, expiry):
    try:
        close = expiration_dict[ticker][expiry]
        return close
    except KeyError:
        try:
            close_day = si.get_data(ticker, start_date=expiry, end_date=expiry+dt.timedelta(days=1), index_as_date=False)
        except:
            expiration_dict[ticker][expiry] = "UNKNOWN"
            return "UNKNOWN"
        close = close_day.loc[0,'adjclose']
        expiration_dict[ticker][expiry] = close
        return close


master_data = pd.DataFrame()

files_read = {}

if os.path.exists('maxTimes.json'):
    with open('maxTimes.json', 'r') as f:
        files_read = json.load(f)

# record ticker - call/put - max pull time in json file

ticker_list = os.listdir('im_vol')
for ticker in tqdm(ticker_list):
    if ticker not in files_read:
        files_read[ticker] = {}
    path = 'im_vol/'+ticker
    for file in os.listdir(path+'/calls'):  
        # check filename against last pull date and hour for ticker      
        f_path = path+'/calls/'+file
        if 'calls' in files_read[ticker]:
            if files_read[ticker]['calls'] < os.path.getmtime(f_path):
                continue
                
        chain = pd.read_csv(f_path)

        chain = chain[['Contract Name','Strike', 'Last Trade Date', 'Pull Timestamp', 'Implied Volatility', 'Volume']]
        chain['Pull Timestamp'] = chain['Pull Timestamp'].apply(dp.parse)
        chain['Last Trade Date'] = chain['Last Trade Date'].apply(dp.parse)
        expiry = get_expiry_from_name(chain['Contract Name'][0], ticker)
        pull_date = chain['Pull Timestamp'][0]
        chain['Expiry'] = expiry
        chain['Ticker'] = ticker
        chain['Days to Expiry'] = chain['Expiry'] - chain['Pull Timestamp']
        chain['Days to Expiry'] = chain['Days to Expiry'].apply(lambda x: x.days)        
        
        '''
        band_data = get_band(ticker, pull_date, band_age=20)
        if band_data == -1:
            print('Yahoo_fin failed, fill data later')            
            continue
        if band_data.empty:
            time.sleep(5)
            band_data = get_band(ticker, pull_date, band_age=20)
        if band_data.empty:
            print('Totally empty')
            time.sleep(2)
            continue
        chain['MA'] = band_data['MA'][0]
        chain['STD'] = band_data['STD'][0]
        chain['Safe'] = chain['Strike'] > band_data['Upper'][0]
        '''

        chain['Contract'] = "C"
        close = get_close_price(ticker, expiry)
        chain['adjclose at expiry'] = close
        master_data = pd.concat([master_data, chain])

        if 'calls' in files_read[ticker]:
            if files_read[ticker]['calls'] < os.path.getmtime(f_path):
                files_read[ticker]['calls'] = os.path.getmtime(f_path)
        else:
            files_read[ticker]['calls'] = os.path.getmtime(f_path)

    for file in os.listdir(path+'/puts'):
        f_path = path+'/puts/'+file
        
        if 'puts' in files_read[ticker]:
            if files_read[ticker]['puts'] > os.

        chain = pd.read_csv(f_path)
        chain = chain[['Contract Name','Strike', 'Last Trade Date', 'Pull Timestamp', 'Implied Volatility', 'Volume']]
        chain['Pull Timestamp'] = chain['Pull Timestamp'].apply(dp.parse)
        chain['Last Trade Date'] = chain['Last Trade Date'].apply(dp.parse)
        expiry = get_expiry_from_name(chain['Contract Name'][0], ticker)
        pull_date = chain['Pull Timestamp'][0]
        chain['Expiry'] = expiry
        chain['Ticker'] = ticker
        chain['Days to Expiry'] = chain['Expiry'] - chain['Pull Timestamp']
        chain['Days to Expiry'] = chain['Days to Expiry'].apply(lambda x: x.days)
        
        '''
        band_data = get_band(ticker, date=pull_date, band_age=20)
        if band_data == -1:
            print('Yahoo_fin failed, fill data later')        
            continue
        if band_data.empty:
            time.sleep(5)
            band_data = get_band(ticker, date=pull_date, band_age=20)
        if band_data.empty:
            print('Totally empty')
            time.sleep(2)
            continue
        chain['MA'] = band_data['MA'][0]
        chain['STD'] = band_data['STD'][0]
        chain['Safe'] = chain['Strike'] < band_data['Lower'][0]
        '''

        chain['Contract'] = "P"
        close = get_close_price(ticker, expiry)
        chain['adjclose at expiry'] = close
        master_data = pd.concat([master_data, chain])

        if 'puts' in files_read[ticker]:
            if files_read[ticker]['puts'] < os.path.getmtime(f_path):
                files_read[ticker]['puts'] = os.path.getmtime(f_path)
        else:
            files_read[ticker]['puts'] = os.path.getmtime(f_path)

master_data['Assigned'] = master_data.apply(is_assigned, axis=1)

master_data.to_csv('latest_dump.csv')
with open('maxTime.json', 'w') as f:
    json.dump(files_read, f)
#master_data.to_json('master_data.json')
