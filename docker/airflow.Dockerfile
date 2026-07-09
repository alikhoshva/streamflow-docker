# Dockerfile for Airflow service
FROM apache/airflow:latest
# Add dependencies or custom configuration here
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow
RUN pip install PyYAML pyspark==3.5.0 apache-airflow-providers-apache-spark