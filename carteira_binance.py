import pandas as pd
from binance import Client
import sqlalchemy
from config import database_infos

client = Client(database_infos['api_key'], database_infos['api_secret'])
engine_carteira_binance = sqlalchemy.create_engine('sqlite:///CARTEIRA_BINANCE.db') #Chamada SQL

# identificar o saldo em conta
def saldo_conta():
    info = client.get_account()
    lista_ativos = info['balances']
    lista_ativos_v = []
    for ativo in lista_ativos:
        if float(ativo['free']) > 0:
            lista_ativos_v.append(ativo)
        if float(ativo['locked']) > 0:
            lista_ativos_v.append(ativo)

    lista_ativos_df = pd.DataFrame(lista_ativos_v)

    frame = lista_ativos_df
    frame.to_sql('CARTEIRA_BINANCE', engine_carteira_binance, if_exists='replace', index=False)


#Pegando todas as moedas e preços com pares em BTC e USDT na binance

all_prices = pd.DataFrame(client.get_all_tickers())
all_prices_tratado = []
all_pair_btc = all_prices[all_prices['symbol'].apply(lambda x: x[-3:] == 'BTC')] #filtrando somente os pares de BTC
all_pair_usdt = all_prices[all_prices['symbol'].apply(lambda x: x[-4:] == 'USDT')] #filtrando somente os pares de USDT
all_prices_tratado.append(all_pair_btc)
all_prices_tratado.append(all_pair_usdt)


print(all_prices_tratado)






