import pandas as pd
import yahoo_fin.stock_info as si
import dateutil.parser as dp
import os
import yahoo as lib
from tqdm import tqdm
import datetime as dt
from collections import defaultdict

#pd.set_option('mode.chained_assignment', None)

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

ticker_list = os.listdir('im_vol')
for ticker in tqdm(ticker_list):
    path = 'im_vol/'+ticker
    for file in os.listdir(path+'/calls'):        
        chain = pd.read_csv(path+'/calls/'+file)
        chain = chain[['Contract Name','Strike', 'Last Trade Date', 'Pull Timestamp', 'Implied Volatility', 'Volume']]
        chain['Pull Timestamp'] = chain['Pull Timestamp'].apply(dp.parse)
        chain['Last Trade Date'] = chain['Last Trade Date'].apply(dp.parse)
        expiry = get_expiry_from_name(chain['Contract Name'][0], ticker)
        pull_date = chain['Pull Timestamp'][0]
        chain['Expiry'] = expiry
        chain['Ticker'] = ticker
        band_data = lib.get_band(ticker, start_date=pull_date, band_age=20)
        chain['MA'] = band_data['MA'][0]
        chain['STD'] = band_data['STD'][0]
        chain['Contract'] = "C"
        chain['Safe'] = chain['Strike'] > band_data['Upper'][0]
        close = get_close_price(ticker, expiry)
        chain['adjclose at expiry'] = close
        master_data = pd.concat([master_data, chain])

    for file in os.listdir(path+'/puts'):
        chain = pd.read_csv(path+'/puts/'+file)
        chain = chain[['Contract Name','Strike', 'Last Trade Date', 'Pull Timestamp', 'Implied Volatility', 'Volume']]
        chain['Pull Timestamp'] = chain['Pull Timestamp'].apply(dp.parse)
        chain['Last Trade Date'] = chain['Last Trade Date'].apply(dp.parse)
        expiry = get_expiry_from_name(chain['Contract Name'][0], ticker)
        pull_date = chain['Pull Timestamp'][0]
        chain['Expiry'] = expiry
        chain['Ticker'] = ticker
        band_data = lib.get_band(ticker, start_date=pull_date, band_age=20)
        chain['MA'] = band_data['MA'][0]
        chain['STD'] = band_data['STD'][0]
        chain['Contract'] = "P"
        chain['Safe'] = chain['Strike'] < band_data['Lower'][0]
        close = get_close_price(ticker, expiry)
        chain['adjclose at expiry'] = close
        master_data = pd.concat([master_data, chain])

master_data['Assigned'] = master_data.apply(is_assigned, axis=1)

master_data.to_csv('master_data.csv')
#master_data.to_json('master_data.json')
