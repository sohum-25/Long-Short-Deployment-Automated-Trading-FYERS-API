import pandas as pd
from StockFetcher import get_long_short_universe
import fyers_apiv3 as fyers
from urllib.parse import urlparse, parse_qs
from fyers_apiv3.FyersWebsocket import data_ws
from fyers_apiv3 import fyersModel
from datetime import datetime, time
import time as sleep_time
import os

def create_order(symbol, side):
    order_dict = {
        "symbol": symbol,
        "qty": 1,
        "type": 2,
        "side": side,
        "productType": "INTRADAY",
        "limitPrice": 0,
        "stopPrice": 0,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
    }
    return order_dict




##Run at 9:15 am
stocks_list = pd.read_csv('ind_nifty500list.csv')['Symbol'].to_list()
long_stocks,short_stocks=get_long_short_universe(stocks_list,5)

long_orders=[]
short_orders=[]

for  i in range(len(long_stocks)):
    long_orders.append(create_order(long_stocks[i],1))

for  i in range(len(short_stocks)):
    short_orders.append(create_order(short_stocks[i],-1))

target_time_long = time(9, 26)
target_time_short = time(9, 15)  
target_time_exit = time(15, 14)

long_executed = False
short_executed = False
exit_executed = False

while True:
    current_time = datetime.now().time()

    if not long_executed and current_time >= target_time_long:
        response_long = fyers.place_basket_orders(data=long_orders)
        print("Long order executed.")
        long_executed = True

    elif not short_executed and current_time >= target_time_short:
        response_short = fyers.place_basket_orders(data=short_orders)
        print("Short order executed.")
        short_executed = True

    elif not exit_executed and current_time >= target_time_exit:
        # Close all positions at exit time
        response_exit = fyers.exit_positions()
        print("Exiting all positions.")
        exit_executed = True
       
        print(fyers.orderbook())
    elif long_executed and short_executed and exit_executed:
        # Break out of the loop once all conditions are executed
        break

    else:
        print(f"Waiting for entry or exit time. Current time: {current_time}")

    sleep_time.sleep(3)