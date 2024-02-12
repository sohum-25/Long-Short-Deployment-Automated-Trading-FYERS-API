import pandas as pd
import yfinance as yf
import warnings
import time

warnings.simplefilter(action='ignore', category=FutureWarning)
def calculate_difference_metric(rolling_values_df, target_date, trade_type='long'):
    target = rolling_values_df.loc[rolling_values_df.index.date < target_date].tail(1).T
    target.rename(columns={target.columns[0]: 'LinearReg'}, inplace=True)
   
    if trade_type == 'long':
        target = target[target['LinearReg'] > 0]
    elif trade_type == 'short':
        target = target[target['LinearReg'] < 0]
    ascending_order = (trade_type == 'short')
    sorted_columns = list(target.sort_values('LinearReg', ascending=ascending_order).index)
    
    return sorted_columns

def get_long_short_universe(stocks, stocks_to_select=2):
    max_retries = 3
    retry_count = 0
    MasterDf = None

    while retry_count < max_retries:
        try:
            MasterDf = yf.download(tickers=stocks, period='30d')[["Close", "Open"]]
            break  
        except Exception as e:
            retry_count += 1
            print(f"Attempt {retry_count} failed. Retrying...")
            time.sleep(0.1)  
    if MasterDf is None:
        print("Error: Unable to fetch stocks today after multiple retries.")

    MasterDf = MasterDf.dropna().dropna(axis=1)
    MasterDf.index = pd.to_datetime(MasterDf.index)
    CloseDf = MasterDf['Close'][:-1]
    OpenDf = MasterDf['Open']
    rolling_values_df = (CloseDf - CloseDf.rolling(5).mean()) / CloseDf
    long_universe = calculate_difference_metric(rolling_values_df, CloseDf.index[-1], 'long')
    short_universe = calculate_difference_metric(rolling_values_df, CloseDf.index[-1], 'short')

    long_universe=list(set(long_universe) & set(CloseDf.columns))
    short_universe=list(set(short_universe) & set(CloseDf.columns))

    CloseDf_long = MasterDf['Close'][long_universe][:-1]
    OpenDf_long = MasterDf['Open'][long_universe]
    CloseDf_short = MasterDf['Close'][short_universe][:-1]
    OpenDf_short = MasterDf['Open'][short_universe]
    
    long_stocks = final_stocks(OpenDf_long, CloseDf_long, OpenDf_long.index[-1], CloseDf_long.index[-1], trade_type='long', lst=long_universe, stocks=stocks_to_select)
    short_stocks = final_stocks(OpenDf_short, CloseDf_short, OpenDf_short.index[-1], CloseDf_short.index[-1], trade_type='short', lst=short_universe, stocks=stocks_to_select)
    
    long_stocks = [f'NSE:{stock[:-3]}-EQ' for stock in long_stocks]
    short_stocks = [f'NSE:{stock[:-3]}-EQ' for stock in short_stocks]


    return long_stocks, short_stocks

def final_stocks(Open_df, Close_df, present_date, past_date, trade_type='long', lst=None,stocks=2):
    if present_date < past_date:
        print('False')
        
    open_prices_present = Open_df.loc[Open_df.index.date == present_date].head(1).T
    close_prices_past = Close_df.loc[Close_df.index.date == past_date].tail(1).T
    gap = (open_prices_present.values - close_prices_past.values) / close_prices_past.values
    
    result_df = pd.DataFrame(gap, columns=['Gap'])    
    result_df['Stocks'] = open_prices_present.index
    result_df.set_index('Stocks', inplace=True)
    result_df.dropna(inplace=True)
    
    ascending_order = (trade_type.lower() == 'long')
    
    result_df.sort_values('Gap', ascending=ascending_order, inplace=True)

    result_df = result_df[(abs(result_df['Gap']) <= 0.087 ) & ~((result_df['Gap'].abs() >= 0.0498) & (result_df['Gap'].abs() <= 0.0501))]
    if lst is not None:
        intersection = result_df.index.to_list()
        return intersection[0:stocks]
    else:
        return []







