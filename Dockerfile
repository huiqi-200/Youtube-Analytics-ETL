
FROM apache/airflow:2.5.1 as airflow-base


# Set environment variables
ENV JAVA_HOME=/opt/bitnami/java
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Give read& write permissions to docker
RUN chmod -R 777 /opt/airflow/ProcessedData && chmod -R 777 /opt/airflow/RawData


ENV PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"
# Install Airflow dependencies
ADD requirements.txt .
RUN pip install -r requirements.txt


# Reference:
# Docker multi stage building https://docs.docker.com/build/building/multi-stage/