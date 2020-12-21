## potentially adding bollinger bands to data later

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
