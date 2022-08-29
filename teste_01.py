import pandas as pd
from binance import Client
import sqlalchemy
from config import database_infos

client = Client(database_infos['api_key'], database_infos['api_secret'])


all_prices = pd.DataFrame(client.get_all_tickers())
all_prices_tratado = []
all_prices_btc = all_prices[all_prices['symbol'].apply(lambda x: x[-3:] == 'BTC')] #filtrando somente os pares de BTC
all_prices_usdt = all_prices[all_prices['symbol'].apply(lambda x: x[-4:] == 'USDT')] #filtrando somente os pares de USDT
all_prices_tratado.append(all_prices_btc)
all_prices_tratado.append(all_prices_usdt)


print(all_prices_tratado)

