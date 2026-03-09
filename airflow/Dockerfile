FROM apache/airflow:3.1.7-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER root

COPY requirements.txt .

# Install Java 17
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID