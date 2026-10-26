import json
import time 
import yfinance as yf 
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers = "kafka:29092", 
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_stock_data(symbol='AAPL'):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="1d", interval = "1m").tail(1)
    if data.empty:
        return None
    row = data.iloc[-1]
    return {
        "symbol":symbol,
        "timestamp":str(data.index[-1]),
        "open": row["Open"],
        "high": row["High"],
        "low" : row["Low"],
        "close":row['Close'],
        "volume": int(row["Volume"])
    }



if __name__ == "__main__":
    while True:
        record = fetch_stock_data("AAPL")
        if record:
            producer.send("stock-prices", value=record)
            print(f"sent: {record}")
        time.sleep(60)




