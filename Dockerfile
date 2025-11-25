#FROM apache/airflow:2.6.0-python3.8
FROM apache/airflow:2.6.0
#FROM apache/airflow:3.0.0-python3.12

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y procps && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

COPY data/postgresql-42.2.5.jar /opt/airflow/postgresql-42.2.5.jar

USER airflow

#RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.0.0
# Instalar Spark Provider y PySpark 3.4.3 (en orden correcto)
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.0.0 \
    pyspark==3.1.3
