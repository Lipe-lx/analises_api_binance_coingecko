from websocket_connection import engine_ws
import pandas as pd
from carteira_binance import engine_carteira_binance
from pycoingecko import CoinGeckoAPI

#Leitura dos dados no BD_websocket e pegando preco mais atualizado
BD_CESTA = pd.read_sql('CESTA_MOEDAS', engine_ws)
BD_CESTA2 = BD_CESTA.copy()
BD_CESTA3 = BD_CESTA2['Close_price'].iloc[-1] #Pega o ultimo valor inserido no banco de dados - Valor mais atualizado
#print(BD_CESTA3)

#Lista de moedas CoinGecko
cg = CoinGeckoAPI()
lista_moedas_CGK = pd.DataFrame(cg.get_coins_markets('USD')) #tabela das 100 primeiras moedas na CoinGecko
lista_moedas_CGK_rend = lista_moedas_CGK[['symbol', 'current_price']] #selecionando somente as culunas desejadas
lista_moedas_CGK_rend['symbol'] = lista_moedas_CGK_rend['symbol'].str.upper() #colocando a coluna inteira com letras maiusculas
print(lista_moedas_CGK_rend)

# Setando o DataFrame da carteira
carteira_binance_setada = pd.read_sql('CARTEIRA_BINANCE', engine_carteira_binance)
carteira_binance_setada = carteira_binance_setada.rename(columns={'asset': 'symbol'}) #Setando o nome da coluna para realizar o merge (procv)
carteira_binance_setada = pd.merge(carteira_binance_setada, lista_moedas_CGK_rend, on=['symbol'], how='left') #PROCV no pandas, primeiro seleciona a planilha base, depois a planilha com os dados a se inserir, depois a coluna em comum setada com o mesmo nome e por ultimo a posição da coluna

carteira_binance_setada['current_price'] = carteira_binance_setada['current_price'].astype(float)
carteira_binance_setada['free'] = carteira_binance_setada['free'].astype(float)
carteira_binance_setada['locked'] = carteira_binance_setada['locked'].astype(float)

carteira_binance_setada['saldo_USD'] = carteira_binance_setada['free'] * carteira_binance_setada['current_price']

print(carteira_binance_setada)

