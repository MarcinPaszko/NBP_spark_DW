FROM apache/airflow:2.7.1

USER root


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    ca-certificates && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow


COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt