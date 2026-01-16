from yahooquery import Ticker
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date, timedelta
import sys


SYMBOLS = [
    '11B.WA', 'AAPL', 'ABBV', 'ACP.WA', 'ADA-USD', 'ADBE', 'ALE.WA', 'AMZN', 'APT-USD', 'ASB.WA',
    'ATOM-USD', 'ATT.WA', 'AVAX-USD', 'AVGO', 'BAC', 'BCH-USD', 'BDX.WA', 'BNB-USD', 'BRK-B', 'BTC-USD',
    'CCC.WA', 'CC=F', 'CDR.WA', 'CL=F', 'CORN', 'COST', 'CPS.WA', 'CT=F', 'CVX', 'DAI-USD', 'DBC',
    '^DJI', 'DNP.WA', 'DOGE-USD', 'DOT-USD', 'EEM', 'ETC-USD', 'ETH-USD', 'FAN', '^FCHI', 'FIL-USD',
    '^FTSE', 'GC=F', '^GDAXI', 'GLD', 'GOOGL', 'GPW.WA', '^GSPC', 'HBAR-USD', 'HD', 'HG=F', '^HSI',
    'HYG', 'IWM', '^IXIC', 'JNJ', 'JO', 'JPM', 'JSW.WA', 'KC=F', 'KGH.WA', 'KO', 'KRU.WA', 'KTY.WA',
    'LINK-USD', 'LIT', 'LPP.WA', 'LTC-USD', 'MA', 'MATIC-USD', 'MBK.WA', 'META', 'MSFT', '^N225',
    'NEAR-USD', 'NG=F', 'NVDA', 'OJ=F', 'OPL.WA', 'ORCL', 'PA=F', 'PEO.WA', 'PEP', 'PG', 'PGE.WA',
    'PICK', 'PKN.WA', 'PKO.WA', 'PL=F', 'PZU.WA', 'QQQ', 'REMX', '^RUT', 'SB=F', 'SHIB-USD', 'SI=F',
    'SLV', 'SOL-USD', 'SOYB', 'SPL.WA', 'SPY', '^STOXX50E', 'TAN', 'TLT', 'TPE.WA', 'TRX-USD', 'TSLA',
    'UNH', 'UNI7083-USD', 'UNI-USD', 'URA', 'V', 'VEA', '^VIX', 'VNQ', 'VTI', 'VWO', 'VXUS', 'WEAT',
    'WMT', 'WOOD', 'XLM-USD', 'XMR-USD', 'XRP-USD', 'ZC=F', 'ZW=F'
]

DB_CONF = {
    "url": "jdbc:postgresql://postgres_dw:5432/currency_db",
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

def get_max_dates_dict(spark):
    """Pobiera słownik {ticker: max_date} z bazy danych."""
    try:
        df = spark.read.format("jdbc").options(**DB_CONF).option("dbtable", "f_stock_prices").load()
        
        max_dates = df.groupBy("ticker").agg(F.max("trade_date").alias("max_date")).collect()
        return {row['ticker']: row['max_date'] for row in max_dates}
    except Exception as e:
        print(f"Cannot retrive data from db: {e}")
        return {}

def chunk_list(lst, n):
    """Dzieli listę na mniejsze kawałki."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_and_save():
    
    spark = SparkSession.builder \
        .appName("Stocks_Smart_Incremental") \
        .config("spark.jars", "/opt/airflow/scripts/postgresql-42.7.3.jar") \
        .getOrCreate()

    
    max_dates_map = get_max_dates_dict(spark)
    today = date.today()
    all_data = []

    print(f"Starting checks for {len(SYMBOLS)} symbols...")

    
    for chunk in chunk_list(SYMBOLS, 30):
       
        dates_in_chunk = [max_dates_map.get(s, date(2024, 1, 1)) for s in chunk]
        min_date_in_chunk = min(dates_in_chunk)
        
        start_fetch = min_date_in_chunk + timedelta(days=1)

        
        if start_fetch >= today:
            print(f"PAckage {chunk[:3]}... is actual:(last date {min_date_in_chunk}).")
            continue

        try:
            print(f"Downloading package {chunk[:3]}... from {start_fetch} to {today}")
            t = Ticker(chunk)
            df = t.history(start=start_fetch, end=today)
            
            if not df.empty:
                df = df.reset_index()
                all_data.append(df)
            else:
                print(f"No new data for package {chunk[:3]}...")
        except Exception as e:
            print(f"Error while downloading package {chunk[:3]}: {e}")

    if not all_data:
        print("All days actual, closing process.")
        spark.stop()
        return

    
    full_pdf = pd.concat(all_data)

    
    full_pdf = full_pdf.rename(columns={
        'symbol': 'ticker',
        'date': 'trade_date',
        'open': 'open_price',
        'high': 'high_price',
        'low': 'low_price',
        'close': 'close_price'
    })

    
    full_pdf['trade_date'] = full_pdf['trade_date'].astype(str)

    
    sdf = spark.createDataFrame(full_pdf)
    final_sdf = sdf.select(
        F.col("ticker"),
        F.col("trade_date").cast("date"),
        F.col("open_price").cast("decimal(12,2)"),
        F.col("high_price").cast("decimal(12,2)"),
        F.col("low_price").cast("decimal(12,2)"),
        F.col("close_price").cast("decimal(12,2)"),
        F.col("volume").cast("bigint")
    ).dropDuplicates(["ticker", "trade_date"])

    print(f"Saving {final_sdf.count()} new records to db...")
    
    final_sdf.write.format("jdbc") \
        .options(**DB_CONF) \
        .option("dbtable", "f_stock_prices") \
        .mode("append") \
        .save()
    
    print("--- PROCESS SUCESSFULL ---")
    spark.stop()

if __name__ == "__main__":
    fetch_and_save()