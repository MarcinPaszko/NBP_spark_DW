#  Financial Data Engineering Hub

Kompletny system ETL (Extract, Transform, Load) budujący lokalną hurtownię danych finansowych. Projekt integruje dane z NBP oraz Yahoo Finance, przetwarzając je przy użyciu Apache Spark.

##  Architektura Systemu
Projekt oparty jest na **Medallion Architecture**:
* **Bronze Layer**: Surowe dane JSON/CSV pobrane z API (NBP, Yahoo Finance).
* **Silver Layer**: Dane oczyszczone i ustandaryzowane przez Sparka (konwersja typów, obsługa braków).
* **Gold Layer**: Tabele analityczne w Postgresie gotowe do raportowania w Power BI.



##  Stack Technologiczny
* **Język**: Python (Requests, Pandas)
* **Przetwarzanie**: Apache Spark (PySpark)
* **Baza Danych**: PostgreSQL
* **Konteneryzacja**: Docker & Docker Compose
* **Orkiestracja**: Apache Airflow (w trakcie wdrażania)
* **Wersjonowanie**: Git

##  Zakres Danych
1.  **Waluty (NBP)**: Kursy średnie (Tabela A) oraz spready kupno/sprzedaż (Tabela C).
2.  **Surowce**: Ceny złota (NBP) oraz kontrakty terminowe (Yahoo Finance).
3.  **Giełda & Krypto**: Ponad 120 instrumentów (S&P500, GPW, Bitcoin, NVIDIA, Tesla).

##  Jak uruchomić?
1. Skonfiguruj środowisko: `docker-compose up -d`
2. Wejdź do Jupytera (port 8888) i uruchom skrypty w folderze `scripts/`.
3. Połącz się z bazą Postgres (port 5433) aby odpytać dane.
