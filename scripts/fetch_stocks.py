import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
import pandas as pd

# 1. KONFIGURACJA BAZY
DB_CONF = {
    "url": "jdbc:postgresql://postgres_dw:5432/currency_db",
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Stocks_Airflow_Ingest") \
    .config("spark.jars", "/opt/airflow/scripts/postgresql-42.7.3.jar") \
    .getOrCreate()

# 2. PEŁNA LISTA TICKERÓW (Zgodna z Jupyter Notebook)
GROUPS = {
    "USA": ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B', 'UNH', 'V', 'JNJ', 'WMT', 'JPM', 'MA', 'PG', 'AVGO', 'HD', 'CVX', 'ORCL', 'ABBV', 'KO', 'PEP', 'COST', 'BAC', 'ADBE'],
    "INDEX": ['^GSPC', '^IXIC', '^DJI', '^RUT', '^VIX', '^FTSE', '^GDAXI', '^FCHI', '^N225', '^HSI', '^STOXX50E', 'QQQ', 'SPY', 'IWM', 'EEM', 'VWO', 'VEA', 'VNQ', 'GLD', 'SLV', 'DBC', 'TLT', 'HYG', 'VTI', 'VXUS'],
    "GPW": ['CDR.WA', 'PKO.WA', 'PKN.WA', 'KGH.WA', 'PZU.WA', 'PEO.WA', 'ALE.WA', 'LPP.WA', 'DNP.WA', 'PGE.WA', 'OPL.WA', 'SPL.WA', 'JSW.WA', 'ACP.WA', 'KRU.WA', 'MBK.WA', 'CPS.WA', 'TPE.WA', 'KTY.WA', 'ATT.WA', 'ASB.WA', 'BDX.WA', 'GPW.WA', '11B.WA', 'CCC.WA'],
    "CRYPTO": ['BTC-USD', 'ETH-USD', 'SOL-USD', 'BNB-USD', 'XRP-USD', 'ADA-USD', 'DOGE-USD', 'AVAX-USD', 'DOT-USD', 'TRX-USD', 'LINK-USD', 'SHIB-USD', 'LTC-USD', 'DAI-USD', 'BCH-USD', 'ATOM-USD', 'XLM-USD', 'XMR-USD', 'ETC-USD', 'FIL-USD', 'HBAR-USD', 'NEAR-USD'],
    "COMMODITIES": ['GC=F', 'SI=F', 'CL=F', 'NG=F', 'HG=F', 'ZC=F', 'ZW=F', 'KC=F', 'CC=F', 'CT=F', 'OJ=F', 'PL=F', 'PA=F', 'SB=F', 'SOYB', 'WEAT', 'CORN', 'WOOD', 'LIT', 'REMX', 'TAN', 'FAN', 'PICK', 'URA']
}

TICKERS = [ticker for group in GROUPS.values() for ticker in group]

def ingest_stocks():
    print(f"Rozpoczynam pobieranie danych dla {len(TICKERS)} tickerów...")
    
    # Pobieramy dane (zmienisz end na 2026-01-09 przy jutrzejszym teście)
    raw_data = yf.download(TICKERS, start="2025-12-01", end="2026-01-08", interval="1d")
    
    if raw_data.empty:
        print("!!! UWAGA: Yahoo Finance nie zwróciło danych. Kończę zadanie.")
        return

    # Reorganizacja danych do formatu tabelarycznego
    try:
        # Close Price - Mapowanie na trade_date i close_price (zgodnie z Postgres)
        df_pd = raw_data['Close'].stack().reset_index()
        df_pd.columns = ['trade_date', 'ticker', 'close_price']
        
        # Volume
        vol_pd = raw_data['Volume'].stack().reset_index()
        vol_pd.columns = ['trade_date', 'ticker', 'volume']
        
        # Merge
        final_pd = df_pd.merge(vol_pd, on=['trade_date', 'ticker'])
        
    except Exception as e:
        print(f"Błąd podczas reorganizacji Pandas: {e}")
        return

    if final_pd.empty:
        print("Brak danych po zmergowaniu. Kończę.")
        return

    # Konwersja do Spark
    df_spark = spark.createDataFrame(final_pd)
    
    # 3. DOPASOWANIE TYPÓW DO BAZY (numeric 12,2 dla cen)
    df_to_save = df_spark.select(
        F.col("ticker"),
        F.col("trade_date").cast("date"),
        F.col("close_price").cast("decimal(12,2)"),
        F.col("volume").cast("long")
    )

    # 4. POBRANIE MAX DATY Z BAZY (używamy kolumny trade_date)
    try:
        db_df = spark.read.format("jdbc").options(**DB_CONF).option("dbtable", "f_stock_prices").load()
        max_db_date = db_df.select(F.max("trade_date")).collect()[0][0]
        if max_db_date is None:
            max_db_date = date(2020, 1, 1)
    except:
        print("Tabela f_stock_prices pusta lub nie istnieje. Start od 2020.")
        max_db_date = date(2020, 1, 1)

    # Filtrowanie i zapis
    new_records = df_to_save.filter(F.col("trade_date") > max_db_date)
    count = new_records.count()

    if count > 0:
        print(f"Dodaję {count} nowych rekordów do bazy...")
        new_records.write.format("jdbc") \
            .options(**DB_CONF) \
            .option("dbtable", "f_stock_prices") \
            .mode("append").save()
        print("Sukces.")
    else:
        print("Brak nowych dat do dodania.")

if __name__ == "__main__":
    ingest_stocks()