FROM apache/airflow:2.7.1

USER root

# 1. Instalacja Javy (niezbędna do działania PySpark wewnątrz Airflow)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Ustawienie zmiennej JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# 3. Instalacja bibliotek z Twojego pliku requirements.txt
# Upewnij się, że w requirements.txt masz: yfinance, pyspark, psycopg2-binary
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt