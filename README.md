# Financial Data Engineering Hub

A complete ETL (Extract, Transform, Load) system designed to build a local financial data warehouse. The project integrates data from NBP (National Bank of Poland) and Yahoo Finance, processing it using Apache Spark.

##  System Architecture
The project follows the **Medallion Architecture**:

* **Bronze Layer**: Raw JSON/CSV data ingested directly from APIs (NBP, Yahoo Finance).
* **Silver Layer**: Cleaned and standardized data processed via Spark (type conversion, handling missing values).
* **Gold Layer**: Analytical tables stored in PostgreSQL, optimized for reporting in Power BI.

##  Tech Stack
* **Language**: Python (Requests, Pandas)
* **Processing**: Apache Spark (PySpark)
* **Database**: PostgreSQL
* **Containerization**: Docker & Docker Compose
* **Orchestration**: Apache Airflow (In progress)
* **Version Control**: Git

##  Data Scope
1.  **Currencies (NBP)**: Average exchange rates (Table A) and buy/sell spreads (Table C).
2.  **Commodities**: Gold prices (NBP) and futures contracts (Yahoo Finance).
3.  **Stock Market & Crypto**: Over 120 instruments (S&P 500, GPW, Bitcoin, NVIDIA, Tesla).

##  How to Run
1.  **Set up the environment**: 
    ```bash
    docker-compose up -d
    ```
2.  **Run Processing**: Open Jupyter Notebook (port `8888`) and run the scripts in the `scripts/` folder.
3.  **Database Access**: Connect to Postgres (port `5433`) or use the terminal:
    ```bash
    docker exec -it postgres_dw psql -U admin -d currency_db
    ```
4.  **List tables**: 
    ```sql
    \dt
    ```