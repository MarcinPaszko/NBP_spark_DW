import requests
import json
from datetime import date, timedelta
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CURRENCIES = ['THB', 'USD', 'AUD', 'HKD', 'CAD', 'NZD', 'SGD', 'EUR', 'CHF', 'HUF',
              'GBP', 'UAH', 'JPY', 'CZK', 'DKK', 'ISK', 'NOK', 'SEK', 'RON', 'TRY',
              'CLP', 'PHP', 'MXN', 'ZAR', 'BRL', 'MYR', 'IDR', 'INR', 'KRW', 'CNY']

SPREAD_CURRENCIES = ['USD', 'EUR', 'CHF', 'GBP', 'CZK']

BASE_PATH = Path("/opt/airflow/data/bronze/incremental_nbp")
BASE_PATH.mkdir(parents=True, exist_ok=True)

spark = SparkSession.builder \
    .appName("NBP_Incremental_Final") \
    .config("spark.jars", "/opt/airflow/scripts/postgresql-42.7.3.jar") \
    .getOrCreate()

DB_CONF = {
    "url": "jdbc:postgresql://postgres_dw:5432/currency_db",
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

def get_max_date(table_name, date_col):
    try:
        df = spark.read.format("jdbc").options(**DB_CONF).option("dbtable", table_name).load()
        res = df.select(F.max(date_col)).collect()[0][0]
        return res if res is not None else date(2020, 1, 1)
    except Exception:
        return date(2020, 1, 1)


last_rates_date = get_max_date("f_currency_rates", "exchange_date")
last_spreads_date = get_max_date("f_currency_spreads", "exchange_date")
last_gold_date = get_max_date("f_gold_prices", "exchange_date")

res_nbp = requests.get("http://api.nbp.pl/api/exchangerates/tables/a/?format=json")
api_date = date.fromisoformat(res_nbp.json()[0]['effectiveDate'])
min_last_date = min(last_rates_date, last_spreads_date, last_gold_date)

if api_date <= min_last_date:
    print(f"NBP data on time ({api_date}).")
else:
    start_fetch = (min_last_date + timedelta(days=1)).strftime('%Y-%m-%d')
    end_fetch = api_date.strftime('%Y-%m-%d')
    
    
    for symbol in CURRENCIES:
       
        url_a = f"http://api.nbp.pl/api/exchangerates/rates/a/{symbol}/{start_fetch}/{end_fetch}/?format=json"
        res_a = requests.get(url_a)
        if res_a.status_code == 200:
            with open(BASE_PATH / f"rate_{symbol}_{end_fetch}.json", 'w') as f:
                json.dump(res_a.json(), f)
        
        if symbol in SPREAD_CURRENCIES:
            url_c = f"http://api.nbp.pl/api/exchangerates/rates/c/{symbol}/{start_fetch}/{end_fetch}/?format=json"
            res_c = requests.get(url_c)
            if res_c.status_code == 200:
                with open(BASE_PATH / f"spread_{symbol}_{end_fetch}.json", 'w') as f:
                    json.dump(res_c.json(), f)

    
    try:
        df_rates = spark.read.option("multiLine", "true").json(f"{BASE_PATH}/rate_*_{end_fetch}.json")
        final_rates = df_rates.select(F.col("code").alias("currency_code"), F.explode("rates").alias("r")).select(
            "currency_code",
            F.col("r.effectiveDate").cast("date").alias("exchange_date"),
            F.col("r.mid").cast("decimal(10,4)").alias("rate_value")
        ).filter(F.col("exchange_date") > last_rates_date)
        if final_rates.count() > 0:
            final_rates.write.format("jdbc").options(**DB_CONF).option("dbtable", "f_currency_rates").mode("append").save()
    except Exception as e: print(f"Error currencies: {e}")

    #
    try:
        df_spreads = spark.read.option("multiLine", "true").json(f"{BASE_PATH}/spread_*_{end_fetch}.json")
        final_spreads = df_spreads.select(F.col("code").alias("currency_code"), F.explode("rates").alias("r")).select(
            F.col("currency_code"),
            F.col("r.effectiveDate").cast("date").alias("exchange_date"),
            F.col("r.bid").cast("decimal(10,4)").alias("bid_price"),
            F.col("r.ask").cast("decimal(10,4)").alias("ask_price")
        ).withColumn("spread_value", F.col("ask_price") - F.col("bid_price")) \
         .filter(F.col("exchange_date") > last_spreads_date)
        if final_spreads.count() > 0:
            final_spreads.write.format("jdbc").options(**DB_CONF).option("dbtable", "f_currency_spreads").mode("append").save()
    except Exception as e: print(f"Spreads error: {e}")

   
    try:
        url_gold = f"http://api.nbp.pl/api/cenyzlota/{start_fetch}/{end_fetch}/?format=json"
        res_gold = requests.get(url_gold)
        if res_gold.status_code == 200:
            df_gold = spark.createDataFrame(res_gold.json())
            final_gold = df_gold.select(
                F.lit("XAU").alias("currency_code"),
                F.col("data").cast("date").alias("exchange_date"),
                F.col("cena").cast("decimal(10,2)").alias("rate_value")
            ).filter(F.col("exchange_date") > last_gold_date)
            if final_gold.count() > 0:
                final_gold.write.format("jdbc").options(**DB_CONF).option("dbtable", "f_gold_prices").mode("append").save()
    except Exception as e: print(f"Gold Error: {e}")

print("NBP Pipeline finished.")