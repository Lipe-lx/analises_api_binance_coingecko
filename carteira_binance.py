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
all_pair_btc = all_prices[all_prices['symbol'].apply(lambda x: x[-3:] == 'BTC')] #filtrando somente os pares de BTC
all_pair_usdt = all_prices[all_prices['symbol'].apply(lambda x: x[-4:] == 'USDT')] #filtrando somente os pares de USDT


#Estruturando o DataFrame da carteira com valores
#Chamar a função 'saldo_conta()' para atualizar o banco de dados

carteira_binance_setada = pd.read_sql('CARTEIRA_BINANCE', engine_carteira_binance)
carteira_binance_setada = carteira_binance_setada.rename(columns={'asset': 'symbol'}) #Setando o nome da coluna para realizar o merge (procv)

carteira_binance_setada_btc = carteira_binance_setada.copy()
carteira_binance_setada_btc['symbol'] = carteira_binance_setada_btc['symbol'] +'BTC' #adicionar BTC como ultimo caracter na coluna symbol (padronização dos dados)
carteira_binance_setada_usdt = carteira_binance_setada.copy()
carteira_binance_setada_usdt['symbol'] = carteira_binance_setada_usdt['symbol'] +'USDT'#adicionar BTC como ultimo caracter na coluna symbol (sufixo) (padronização dos dados)

carteira_binance_pair_btc = pd.merge(carteira_binance_setada_btc, all_pair_btc, on=['symbol'], how='left') #PROCV no pandas, primeiro seleciona a planilha base, depois a planilha com os dados a se inserir, depois a coluna em comum setada com o mesmo nome e por ultimo a posição da coluna
carteira_binance_pair_btc['symbol'] = carteira_binance_pair_btc['symbol'].replace(['USDTBTC', 'BUSDBTC', 'BRLBTC'], ['BTCUSDT', 'BTCBUSD', 'BTCBRL']) #Substitir valores especificos na linha


carteira_binance_pair_usdt = pd.merge(carteira_binance_setada_usdt, all_pair_usdt, on=['symbol'], how='left') #PROCV no pandas, primeiro seleciona a planilha base, depois a planilha com os dados a se inserir, depois a coluna em comum setada com o mesmo nome e por ultimo a posição da coluna
carteira_binance_pair_usdt['symbol'] = carteira_binance_pair_usdt['symbol'].replace(['USDTUSDT', 'BUSDUSDT', 'BRLUSDT'], ['USDT', 'USDTBUSD', 'USDTBRL']) #Substitir valores especificos na linha

print(carteira_binance_pair_btc)
print(carteira_binance_pair_usdt)








