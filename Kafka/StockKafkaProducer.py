import numpy as np
import yfinance as yf
import os
import time
from confluent_kafka import Producer


def main():
    bootstrap_server = "localhost:9092"
    kafka_topic = 'stock_topic'

    producer_config = {
        'bootstrap.servers': bootstrap_server,
        'client.id': 'stock_data_producer'
    }

    producer = Producer(producer_config)

    while (1 == 1):
        for crypt in ['UBER','MSFT','AMZN','GOOG']:
            data = yf.download(tickers=crypt, period='2h', interval='1m')
            print(data.info())
            data.tail(1)
            data['Type'] = crypt
            data['Datetime'] = data.index
            print(data)

            for index, row in data.iterrows():
                print(row)
                message = f"{row['Type']},{row['Close']},{row['Datetime']}"
                producer.produce(kafka_topic, key="1", value=message)

            print(type(data))
            time.sleep(30)


if __name__=="__main__":
    main()