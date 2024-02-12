import schedule
import time as sys_time

def job():
    print("Executing script at", sys_time.strftime('%H:%M'))
    import pandas as pd 
    import os
    from datetime import datetime, time as dt_time, datetime
    from fyers_apiv3 import fyersModel
    from StockFetcher import get_long_short_universe
    import time as sleep_time
    from IPython.display import clear_output
    from datetime import datetime
    import warnings
    import time
    warnings.simplefilter(action='ignore', category=FutureWarning)

    log_file_path = r'C:\Users\Sohum\Desktop\Long Short Deployment\log_file.txt'

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"This task was last run on {timestamp}.\n"

    with open(log_file_path, 'a') as log_file:
        log_file.write(log_message)

    from Autologin import generateToken
    client_id = "CDAAGPIZ8E-100"
    access_token = generateToken()
    while not access_token:
        print("Failed to obtain access token. Retrying...")
        access_token = generateToken()
    fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path="")
    print(fyers.funds()['fund_limit'][8]['equityAmount'])
    def date_to_epoch():
        current_date_time = datetime.now()
        epoch_time_current = int(current_date_time.timestamp())
        return epoch_time_current

    def create_order(symbol, side, capital):
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
        quantity = int(capital/openprice)
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

    target_time_open = dt_time(9, 13)
    target_time_long = dt_time(9, 15)
    target_time_short = dt_time(9, 15)
    target_time_exit = dt_time(15, 14, 0)
    stocks_list = pd.read_csv('ind_nifty500list.csv')['Symbol'].to_list()
    long_orders = []
    short_orders = []

    while True:
        current_time = datetime.now().time()
        if current_time >= target_time_open:
            long_stocks, short_stocks = get_long_short_universe(stocks_list, 2)
            print(long_stocks, short_stocks)
            Master_capital = 5 * int(fyers.funds()['fund_limit'][8]['equityAmount'])
            partial_capital = Master_capital / (2 * len(long_stocks))
            for i in range(len(long_stocks)):
                long_orders.append(create_order(long_stocks[i], 1, partial_capital))

            for i in range(len(short_stocks)):
                short_orders.append(create_order(short_stocks[i], -1, partial_capital))
            print(f"Capital Allocated per position: {partial_capital}")
            break
        else:
            print('Waiting for the Day to begin')

        sleep_time.sleep(0.5)
        clear_output(wait=True)

    long_executed = False
    short_executed = False
    exit_executed = False

    while True:
        current_time = datetime.now().time()

        if not long_executed and current_time >= target_time_long:
            response_long = fyers.place_basket_orders(data=long_orders)
            print("Long order executed.")
            print(response_long)
            long_executed = True

        elif not short_executed and current_time >= target_time_short:
            response_short = fyers.place_basket_orders(data=short_orders)
            print(response_short)
            print("Short order executed.")
            short_executed = True

        elif not exit_executed and current_time >= target_time_exit:
            response_exit = fyers.exit_positions()
            print(response_exit)
            print("Exiting all positions.")
            exit_executed = True

            df = pd.DataFrame(fyers.orderbook()['orderBook'])
            df = df[['id', 'qty', 'symbol', 'orderDateTime', 'side', 'tradedPrice', 'orderNumStatus', 'slNo']]

            project_folder = r"C:\Users\Sohum\Desktop\Long Short Deployment"

            csv_file_path = os.path.join(project_folder, "tradebook.csv")

            df.to_csv(csv_file_path, mode='a', header=not os.path.exists(csv_file_path), index=False)

            print("DataFrame appended to CSV:", csv_file_path)
        elif long_executed and short_executed and exit_executed:
            break

        else:
            print(f"Waiting for entry or exit time. Current time: {current_time}")
            clear_output(wait=True)
        sleep_time.sleep(0.1)

scheduled_time = "09:10"

# Schedule the job
schedule.every().day.at(scheduled_time).do(job)

while True:
    schedule.run_pending()
    print("Waiting for the markets to open...")
    sys_time.sleep(0.5)
    