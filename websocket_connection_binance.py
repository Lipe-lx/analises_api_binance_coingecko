import json
import websocket
import sqlalchemy #banco de dados SQL ALchemy
import pandas as pd

#websocket.enableTrace(True) #descomente se for debugar
engine_ws = sqlalchemy.create_engine('sqlite:///CESTA_MOEDAS_raw.db') #Chamada SQL

time_char = '1m'
coin_01 = 'ethusdt'
coin_02 = 'btcusdt'
coin_03 = 'bnbusdt'
coin_04 = 'ethbtc'

pair_coins = f'{coin_01}@kline_{time_char}/{coin_02}@kline_{time_char}/{coin_03}@kline_{time_char}/{coin_04}@kline_{time_char}'

def websocket_chamada():
        def on_open(ws):
            print("open")

        def on_message(ws, message):
                json_message = json.loads(message) #recebimento da msg raiz (raw) ja em dicionario json
                candle = json_message['data']['k'] #selecinar um dicioanrio somente com as datas e o dicionario 'k'

                df = pd.DataFrame([candle])

                #print(df.keys()) # DEBUG: verificando a chave e valores para filtros / base de consulta em https://binance-docs.github.io/apidocs/spot/en/#trade-streams
                #print(df.values()) # DEBUG:

                df = df.loc[:, ['t', 'T', 's', 'i', 'f', 'L', 'o', 'c', 'h', 'l', 'v', 'n', 'x', 'q', 'V', 'Q', 'B']]  #Parametros da API Binance, abaixo o significado de cada um
                df.columns = ['start_time', 'close_time', 'Symbol', 'Interval', ' First_trade_ID', 'Last_trade_ID', 'Open_price', 'Close_price', 'High_price', 'Low_price', 'Base_asset_volume', 'Number_of_trades', 'Candle_close_price', 'Quote_asset_volume', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore']  #Renomeando as colunas
                df = df.loc[:, ['close_time', 'Symbol', 'Close_price']] #Filtrando colunas que quero que apareça
                df.Close_price = df.Close_price.astype(float)  #Passando de str para float a coluna Price
                df.close_time = pd.to_datetime(df.close_time, unit='ms')  #formatação de data
                print(df) #visualizar o DF no terminal

                #Salvando os dados no SQL
                frame = df
                frame.to_sql('CESTA_MOEDAS', engine_ws, if_exists='append', index=False)

        def on_close(ws, close_status_code, close_msg):
            print("closed")

        #Rodando o WebSocket
        SOCK = f"wss://stream.binance.com:9443/stream?streams={pair_coins}"
        ws = websocket.WebSocketApp(SOCK, on_open=on_open, on_close=on_close, on_message=on_message)
        ws.run_forever()

