import pandas as pd 
import os
from datetime import datetime, time
from fyers_apiv3 import fyersModel
from StockFetcher import get_long_short_universe
import pandas as pd
import yfinance as yf
import warnings
import os
import time as sleep_time
from IPython.display import clear_output
from datetime import datetime
warnings.simplefilter(action='ignore', category=FutureWarning)

def getIntradayBrokerage(buyAt,sellAt,quantity):
    brokerage = min((buyAt * quantity * 0.0003),20) + min((sellAt * quantity * 0.0003),20)
    turnover = (buyAt+sellAt)*quantity
    stt = ((sellAt * quantity) * 0.00025)
    sebi = turnover*0.000001
    ex_txn_charge = (0.0000325*turnover) + (0.000001 * turnover)
    stax = (0.18 * (brokerage + ex_txn_charge + sebi))
    stamp = (buyAt*quantity)*0.00003
    return brokerage + stt + ex_txn_charge + stax + sebi + stamp





log_file_path = r'C:\Users\Sohum\Desktop\Long Short Deployment\log_file.txt'
directory_capital = r"C:\Users\Sohum\Desktop\Long Short Deployment"

timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
log_message = f"This task was last run on {timestamp}.\n"

with open(log_file_path, 'a') as log_file:
    log_file.write(log_message)

from Autologin2 import generateToken
client_id = "R4NUGDSC7E-100"
access_token = generateToken()
while not access_token:
    print("Failed to obtain access token. Retrying...")
    access_token = generateToken()
fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path="")
print(f"Global Account Balance:{fyers.funds()['fund_limit'][8]['equityAmount']}")
def date_to_epoch():
    current_date_time = datetime.now()
    epoch_time_current = int(current_date_time.timestamp())
    return epoch_time_current


def create_order(symbol, side, capital):
    try:
        data = {
            "symbol": symbol,
            "resolution": "D",
            "date_format": "0",
            "range_from": date_to_epoch(),
            "range_to": date_to_epoch(),
            "cont_flag": "1"
        }
        response = fyers.history(data=data)
        openprice = float(pd.DataFrame(response['candles'], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])['Open'])
        quantity = int(capital / openprice)
        order_dict = {
            "symbol": symbol,
            "qty": quantity,
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
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

target_time_open = time(9, 15)
target_time_long = time(9, 15)
target_time_short = time(9, 15)  
target_time_exit = time(15, 14,0)
stocks_list = pd.read_csv('ind_nifty500list.csv')['Symbol'].to_list()
long_orders=[]
short_orders=[]

while True:
    current_time = datetime.now().time()
    positions_per_side=4
    if(current_time>=target_time_open):
        long_stocks,short_stocks=get_long_short_universe(stocks_list,6)
        long_stocks=long_stocks[0:4]
        short_stocks=short_stocks[0:4]
        print(long_stocks,short_stocks)
        file_path = os.path.join(directory_capital, "InitialCapital.txt")
        with open(file_path, "r") as file:
            retrieved_result = int(float(file.read()))
        

        Master_capital=5* retrieved_result
        partial_capital=Master_capital/(2*len(long_stocks))
        for  i in range(len(long_stocks)):
            long_orders.append(create_order(long_stocks[i],1,partial_capital))

        for  i in range(len(short_stocks)):
            short_orders.append(create_order(short_stocks[i],-1,partial_capital))
        print(f"Capital Allocated per position : {partial_capital}")
        break
    else:
        print('Waiting for the Day to begin...')
        
    sleep_time.sleep(0.5)
    clear_output(wait=True)



long_executed = True
short_executed = True
exit_executed = False

while True:
    current_time = datetime.now().time()

    if not long_executed and current_time >= target_time_long:
        
        response_long = fyers.place_basket_orders(data=long_orders)
        print("Long order executed: ",response_long)
        long_executed = True

    elif not short_executed and current_time >= target_time_short:
        response_short = fyers.place_basket_orders(data=short_orders)
        print("Short order executed: ",response_short)
        short_executed = True

    elif not exit_executed and current_time >= target_time_exit:
        # Close all positions at exit time
        response_exit = fyers.exit_positions()
        print("Exiting all positions.")
        exit_executed = True

        
        
        df = pd.DataFrame(fyers.orderbook()['orderBook'])
        df['Strategy']="SohumLongShort"
        project_folder = r"C:\Users\Sohum\Desktop\Long Short Deployment"

        csv_file_path = os.path.join(project_folder, "tradebook2.csv")

        df.to_csv(csv_file_path, mode='a', header=not os.path.exists(csv_file_path), index=False)

        print("DataFrame appended to CSV:", csv_file_path)


        Trades=pd.read_csv('tradebook2.csv')

        Trades['orderDateTime']=pd.to_datetime(Trades['orderDateTime'])
        Trades = Trades.loc[(Trades['orderDateTime'].dt.date == datetime.today().date()) & (Trades['filledQty'] != 0)]
        Trades=Trades.loc[Trades['filledQty']!=0]
        Exits,Entries=Trades.loc[Trades['orderTag']=='2:Exit'],Trades.loc[Trades['orderTag']!='2:Exit']


        lst = []
        for i in range(len(Entries['symbol'])):
            stock = Entries['symbol'].iloc[i]
            
            # Check if the symbol exists in both Entry and Exit DataFrames
            if stock in Entries['symbol'].values and stock in Exits['symbol'].values:
                stock_entry = Entries.loc[Entries['symbol'] == stock]
                stock_exit = Exits.loc[Exits['symbol'] == stock]

                lst.append((stock,
                            stock_entry['tradedPrice'].iloc[0],
                            stock_exit['tradedPrice'].iloc[0],
                            stock_entry['side'].iloc[0],
                            stock_entry['filledQty'].iloc[0]))
            else:
                continue


        BookForTheDay=pd.DataFrame(lst,columns=['Stock','Entry','Exit','Side','Quantity'])
        BookForTheDay['Brokerage']=0
        BookForTheDay['Brokerage']=BookForTheDay.apply(lambda row: getIntradayBrokerage(row['Entry'], row['Exit'], row['Quantity']), axis=1)
        BookForTheDay.drop_duplicates('Stock',inplace=True)
        BookForTheDay['Date'] = datetime.now().date()
        folder_path = r"C:\Users\Sohum\Desktop\Long Short Deployment"

        # Create the folder if it doesn't exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # CSV file path
        csv_file = os.path.join(folder_path, "TradeList.csv")

        # Check if the file exists
        try:
            # If file exists, append data
            BookForTheDay.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)
        except FileNotFoundError:
            # If file doesn't exist, create a new file and save data
            BookForTheDay.to_csv(csv_file, index=False)


        nettbrokerage=BookForTheDay['Brokerage'].sum()
        daily_profit = fyers.positions()["overall"]['pl_realized']
        result = ((Master_capital)/5 + daily_profit) - nettbrokerage
        print(f'DailyProfit: {daily_profit} ,brokerage: {nettbrokerage}, Capital Tomorrow: {result}')
        print(f'Capital for the next day: {result}')  
        if not os.path.exists(directory_capital):
            os.makedirs(directory_capital)
        
        file_path = os.path.join(directory_capital, "InitialCapital.txt")
        with open(file_path, "w") as file:
            file.write(str(result))



    elif long_executed and short_executed and exit_executed:
        break

    else:
        print(f"Waiting for entry or exit time. Current time: {current_time}")
        clear_output(wait=True)
    sleep_time.sleep(0.1)