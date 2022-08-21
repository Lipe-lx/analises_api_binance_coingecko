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


#ex. chamada direta na API para coleta de preço:
PAR_MOEDA = 'BTCUSDT'
btc_price = client.get_symbol_ticker(symbol=PAR_MOEDA)

