import pandas as pd
from binance import Client
import sqlalchemy
from config import database_infos

client = Client(database_infos['api_key'], database_infos['api_secret'])
engine_carteira_binance = sqlalchemy.create_engine('sqlite:///CARTEIRA_BINANCE.db') # Chamada SQL

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


# CHAMADA EXTERNA - Pegando todas as moedas e preços com pares em BTC e USDT na binance
# Moedas FIAT
ticker_fiat_BTCUSDT = client.get_ticker(symbol='BTCUSDT')
BTCUSDT_df = pd.DataFrame({'symbol': ticker_fiat_BTCUSDT['symbol'], 'price': ticker_fiat_BTCUSDT['lastPrice']}, index=[0])
ticker_fiat_BTCBUSD = client.get_ticker(symbol='BTCBUSD')
BTCBUSD_df = (pd.DataFrame({'symbol': ticker_fiat_BTCBUSD['symbol'], 'price': ticker_fiat_BTCBUSD['lastPrice']}, index=[0]))
ticker_fiat_BTCUSDC = client.get_ticker(symbol='BTCUSDC')
BTCUSDC_df = (pd.DataFrame({'symbol': ticker_fiat_BTCUSDC['symbol'], 'price': ticker_fiat_BTCUSDC['lastPrice']}, index=[0]))
ticker_fiat_BTCBRL = client.get_ticker(symbol='BTCBRL')
BTCBRL_df = (pd.DataFrame({'symbol': ticker_fiat_BTCBRL['symbol'], 'price': ticker_fiat_BTCBRL['lastPrice']}, index=[0]))
ticker_fiat_USDTBRL = client.get_ticker(symbol='USDTBRL')
USDTBRL_df = (pd.DataFrame({'symbol': ticker_fiat_USDTBRL['symbol'], 'price': ticker_fiat_USDTBRL['lastPrice']}, index=[0]))
ticker_fiat_BUSDBRL = client.get_ticker(symbol='BUSDBRL')
BUSDBRL_df = (pd.DataFrame({'symbol': ticker_fiat_BUSDBRL['symbol'], 'price': ticker_fiat_BUSDBRL['lastPrice']}, index=[0]))

fiat = pd.concat([BTCBRL_df, BTCUSDC_df, BTCBUSD_df, BTCUSDT_df, USDTBRL_df, BUSDBRL_df], ignore_index=True)

#Criptos
all_coins = pd.DataFrame(client.get_all_tickers())
all_coins_and_fiat = pd.concat([fiat, all_coins], ignore_index=True)
#print(all_coins_and_fiat)

all_pair_btc = all_coins[all_coins['symbol'].apply(lambda x: x[-3:] == 'BTC')] # filtrando somente os pares de BTC (tres ultimos caracteres)
#verifica = 'planilha.xlsx'
#all_pair_btc.to_excel(verifica)
#print(all_pair_btc)
all_pair_usdt = all_coins[all_coins['symbol'].apply(lambda x: x[-4:] == 'USDT')] # filtrando somente os pares de USDT (quatro ultimos caracteres)


# CHAMADA INTERNA Estruturando o DataFrame da carteira com valores
# Chamar a função 'saldo_conta()' para atualizar o banco de dados

carteira_binance_setada = pd.read_sql('CARTEIRA_BINANCE', engine_carteira_binance)
carteira_binance_setada = carteira_binance_setada.rename(columns={'asset': 'symbol'}) # Setando o nome da coluna para realizar o merge (procv)

# Carteira em Dollar
carteira_binance_setada_usdt = carteira_binance_setada.copy()
carteira_binance_setada_usdt['symbol'] = (carteira_binance_setada_usdt['symbol'] +'USDT')# adicionar BTC como ultimo caracter na coluna symbol (sufixo) (padronização dos dados)

carteira_binance_setada_usdt['symbol'] = carteira_binance_setada_usdt['symbol'].replace(['USDTUSDT', 'BRLUSDT'], ['USDTBUSD', 'USDTBRL']) # Substitir valores especificos na linha
carteira_binance_pair_usdt = pd.merge(carteira_binance_setada_usdt, all_coins_and_fiat, on=['symbol'], how='left') # PROCV no pandas, primeiro seleciona a planilha base, depois a planilha com os dados a se inserir, depois a coluna em comum setada com o mesmo nome e por ultimo a posição da coluna


print(all_coins_and_fiat)

print(carteira_binance_pair_usdt)

#print(carteira_binance_pair_btc)



