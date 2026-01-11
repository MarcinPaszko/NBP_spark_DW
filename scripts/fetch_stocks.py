from __future__ import annotations
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date, datetime, timedelta
import pandas as pd
import time

# 1. KONFIGURACJA BAZY
DB_CONF = {
    "url": "jdbc:postgresql://postgres_dw:5432/currency_db",
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

# Inicjalizacja Sparka
spark = SparkSession.builder \
    .appName("Stocks_Final_History_Method") \
    .config("spark.jars", "/opt/airflow/scripts/postgresql-42.7.3.jar") \
    .getOrCreate()

# 2. LISTA TICKERÓW (Pogrupowana dla przejrzystości logów)
GROUPS = {
    "USA": ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B', 'UNH', 'V', 'JNJ', 'WMT', 'JPM', 'MA', 'PG', 'AVGO', 'HD', 'CVX', 'ORCL', 'ABBV', 'KO', 'PEP', 'COST', 'BAC', 'ADBE'],
    "INDEX": ['^GSPC', '^IXIC', '^DJI', '^RUT', '^VIX', '^FTSE', '^GDAXI', '^FCHI', '^N225', '^HSI', '^STOXX50E', 'QQQ', 'SPY', 'IWM', 'EEM', 'VWO', 'VEA', 'VNQ', 'GLD', 'SLV', 'DBC', 'TLT', 'HYG', 'VTI', 'VXUS'],
    "GPW": ['CDR.WA', 'PKO.WA', 'PKN.WA', 'KGH.WA', 'PZU.WA', 'PEO.WA', 'ALE.WA', 'LPP.WA', 'DNP.WA', 'PGE.WA', 'OPL.WA', 'SPL.WA', 'JSW.WA', 'ACP.WA', 'KRU.WA', 'MBK.WA', 'CPS.WA', 'TPE.WA', 'KTY.WA', 'ATT.WA', 'ASB.WA', 'BDX.WA', 'GPW.WA', '11B.WA', 'CCC.WA'],
    "CRYPTO": ['BTC-USD', 'ETH-USD', 'SOL-USD', 'BNB-USD', 'XRP-USD', 'ADA-USD', 'DOGE-USD', 'AVAX-USD', 'DOT-USD', 'TRX-USD', 'LINK-USD', 'SHIB-USD', 'LTC-USD', 'DAI-USD', 'BCH-USD', 'ATOM-USD', 'XLM-USD', 'XMR-USD', 'ETC-USD', 'FIL-USD', 'HBAR-USD', 'NEAR-USD'],
    "COMMODITIES": ['GC=F', 'SI=F', 'CL=F', 'NG=F', 'HG=F', 'ZC=F', 'ZW=F', 'KC=F', 'CC=F', 'CT=F', 'OJ=F', 'PL=F', 'PA=F', 'SB=F', 'SOYB', 'WEAT', 'CORN', 'WOOD', 'LIT', 'REMX', 'TAN', 'FAN', 'PICK', 'URA']
}

TICKERS = [ticker for group in GROUPS.values() for ticker in group]

def ingest_stocks():
    print(f"Rozpoczynam pobieranie danych (METODA RAW/HISTORY) dla {len(TICKERS)} tickerów...")
    
    # Ustalamy zakres dat (podobnie jak w Twoim udanym teście)
    # yfinance history start/end
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=10)
    
    all_data = []
    
    for ticker in TICKERS:
        try:
            # Tworzymy obiekt Ticker (bardziej stabilny niż yf.download)
            t = yf.Ticker(ticker)
            
            # Pobieramy historię
            df = t.history(start=start_dt.strftime('%Y-%m-%d'), 
                           end=end_dt.strftime('%Y-%m-%d'), 
                           interval="1d")
            
            if not df.empty:
                temp_df = df.reset_index()
                temp_df['ticker'] = ticker
                
                # Czyścimy strefę czasową z daty (Spark tego nie lubi)
                if temp_df['Date'].dt.tz is not None:
                    temp_df['Date'] = temp_df['Date'].dt.tz_localize(None)
                
                # Wybieramy i nazywamy kolumny
                temp_df = temp_df[['Date', 'ticker', 'Close', 'Volume']]
                temp_df.columns = ['trade_date', 'ticker', 'close_price', 'volume']
                
                all_data.append(temp_df)
                print(f" OK: {ticker}")
            else:
                print(f" SKIP: {ticker} (brak danych w podanym zakresie)")
            
            # Przerwa, aby uniknąć blokady IP (Rate Limiting)
            time.sleep(0.4)

        except Exception as e:
            print(f" BŁĄD {ticker}: {e}")
            time.sleep(1)

    if not all_data:
        print("!!! KRYTYCZNY BŁĄD: Nie pobrano danych dla żadnego instrumentu.")
        return

    # 3. PRZETWARZANIE W SPARK
    final_pd = pd.concat(all_data, ignore_index=True)
    df_spark = spark.createDataFrame(final_pd)
    
    df_to_save = df_spark.select(
        F.col("ticker"),
        F.col("trade_date").cast("date"),
        F.col("close_price").cast("decimal(12,2)"),
        F.col("volume").cast("long")
    )

    # 4. ZAPIS DO POSTGRESQL (Z filtrowaniem duplikatów)
    try:
        # Sprawdzamy co już mamy w bazie
        db_df = spark.read.format("jdbc").options(**DB_CONF).option("dbtable", "f_stock_prices").load()
        max_db_date = db_df.select(F.max("trade_date")).collect()[0][0]
    except Exception:
        print("Tabela f_stock_prices jest pusta lub nie istnieje. Inicjalizacja...")
        max_db_date = None

    if max_db_date is None:
        max_db_date = date(2020, 1, 1)

    # Tylko rekordy nowsze niż te w bazie
    new_records = df_to_save.filter(F.col("trade_date") > max_db_date)
    count = new_records.count()

    if count > 0:
        print(f"Dodaję {count} nowych wierszy do PostgreSQL...")
        new_records.write.format("jdbc") \
            .options(**DB_CONF) \
            .option("dbtable", "f_stock_prices") \
            .mode("append").save()
        print("--- ZAPIS ZAKOŃCZONY ---")
    else:
        print("--- BRAK NOWYCH DANYCH DO DODANIA ---")

if __name__ == "__main__":
    ingest_stocks()