
# Download Hive
FROM alpine:latest AS hive-downloader
ENV HIVE_VERSION=3.1.3
RUN apk add --no-cache curl
RUN curl -sL "https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz" \
        | tar -xz -C / && \
    mv /apache-hive-$HIVE_VERSION-bin /hive

# Build an image on /data directory 
# WORKDIR /data/airflow


# Use an official Python runtime as a base image
FROM python:3.8.18

# Set environment variable for Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow

# Install system dependencies and pip3
RUN apt-get update && apt-get install -y \
    python3-pip \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean

# Upgrade pip3 (optional, but recommended)
RUN python -m pip install --upgrade pip

# Install Airflow
RUN pip3 install apache-airflow==2.5.1

# USER root

# # Install Airflow (assuming you are installing it)
# RUN pip install apache-airflow --prefix /data/airflow_install

# # Change the shebang line of the Airflow executable
# RUN sed -i '1s|^.*$|#!/usr/bin/python3|' /data/airflow_install/bin/airflow


FROM apache/airflow:2.5.1


# Install Java
USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jre wget supervisor && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Hadoop
ENV HADOOP_VERSION=3.3.1
ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
RUN mkdir -p $HADOOP_HOME && \
    curl -sL "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
        | tar -xz -C /opt/ && \
    chown -R root:root $HADOOP_HOME && \
    rm -rf $HADOOP_HOME/share/doc && \
    mkdir -p $HADOOP_HOME/logs && \
    chmod 777 $HADOOP_HOME/logs

# Install Hive
ENV HIVE_VERSION=3.1.3
ENV HIVE_HOME=/opt/hive-$HIVE_VERSION-bin
ENV HIVE_CONF_DIR=$HIVE_HOME/conf
ENV PATH=$PATH:$HIVE_HOME/bin
RUN mkdir -p $HIVE_HOME
COPY --from=hive-downloader /hive/bin/ $HIVE_HOME/bin/
COPY --from=hive-downloader /hive/conf/ $HIVE_HOME/conf/
COPY --from=hive-downloader /hive/lib/ $HIVE_HOME/lib/
COPY --from=hive-downloader /hive/scripts/ $HIVE_HOME/scripts/


# Install Spark
ENV SPARK_VERSION=3.2.1
ENV SPARK_HADOOP_VERSION=3.2
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz -C /opt/ && \
    rm spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Copy custom files
COPY kafka-clients-3.4.0.jar $SPARK_HOME/jars/kafka-clients-3.4.0.jar
COPY hive-exec-3.1.3.jar $HIVE_HOME/lib/hive-exec-3.1.3.jar
COPY hive-exec-3.1.3.jar $SPARK_HOME/jars/hive-exec-3.1.3.jar
COPY hive-site.xml $HIVE_CONF_DIR/hive-site.xml
COPY hive $HIVE_HOME/bin
COPY hive-site.xml $SPARK_HOME/conf
COPY spark-hive_2.12-$SPARK_VERSION.jar $SPARK_HOME/jars
COPY postgresql-42.5.4.jar $HIVE_HOME/lib/postgresql-42.5.4.jar

# Copy the required JAR files from the hive-downloader stage


# Install Python requirements
WORKDIR /opt/airflow/
COPY requirements.txt .

USER airflow
RUN pip3 install --user -r requirements.txt

# # Install NLTK data 
# # Set the working directory
# WORKDIR /opt/airflow
# # Create a directory for NLTK data
# RUN mkdir -p /opt/airflow/nltk_data

# # Download specific NLTK data during the build
# RUN python -m nltk.downloader -d /opt/airflow/nltk_data punkt
# RUN python -m nltk.downloader -d /opt/airflow/nltk_data stopwords
# RUN python -m nltk.downloader -d /opt/airflow/nltk_data words
# RUN python -m nltk.downloader -d /opt/airflow/nltk_data wordnet
# RUN python -m nltk.downloader -d /opt/airflow/nltk_data omw-1.4
# RUN python -m nltk.downloader -d /opt/airflow/nltk_data averaged_perceptron_tagger


# Switch back to root user to perform other tasks
USER root

# Run init_db.sh
COPY init_db.sh /opt/airflow/init_db.sh
RUN chmod +x /opt/airflow/init_db.sh

# Expose ports
EXPOSE 22
EXPOSE 3000
EXPOSE 9000
EXPOSE 9864
EXPOSE 4040
EXPOSE 9083
EXPOSE 10000
EXPOSE 8000

# Find the correct group for the airflow user
RUN id -gn airflow > /group_name.txt


# Set the permissions for the Airflow directories
RUN chown -R airflow:$(cat /group_name.txt) /opt/airflow


# Set the permissions for the Hadoop directories
RUN chown -R airflow:$(cat /group_name.txt) /opt/hadoop* && \
    chmod 775 /opt/hadoop*

# Create Hadoop data directories
RUN mkdir -p /opt/hadoop/data/dfs/namenode && \
    mkdir -p /opt/hadoop/data/dfs/datanode && \
    chown -R airflow:$(cat /group_name.txt) /opt/hadoop/data && \
    chmod -R 775 /opt/hadoop/data

# Set the permissions for the Hive directories
RUN chown -R airflow:$(cat /group_name.txt) /opt/hive* && \
    chmod 775 /opt/hive*

# Create the /data/hive directory
RUN mkdir -p /data/hive

# Change ownership and permissions of the /data/hive directory
RUN chown -R airflow:$(cat /group_name.txt) /data/hive && \
    chmod 775 /data/hive

RUN chown -R airflow:$(cat /group_name.txt) /var/log/supervisor

# Make the Hive binary executable
RUN chmod +x /opt/hive-3.1.3-bin/bin/hive

RUN mkdir -p /opt/airflow/logs && chown -R airflow:$(cat /group_name.txt) /opt/airflow/logs

# Copy airflow_env.sh to the container
COPY airflow_env.sh /opt/airflow/airflow_env.sh
# Copy supervisor configuration file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY entrypoint.sh /entrypoint.sh
COPY core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml

RUN chmod +x /entrypoint.sh
RUN cat /entrypoint.sh

RUN if ! getent group airflow > /dev/null; then \
        addgroup --gid 1000 airflow; \
    fi && \
    if ! getent passwd airflow > /dev/null; then \
        adduser --uid 1000 --ingroup airflow --home /opt/airflow --no-create-home airflow; \
    fi


RUN chown -R airflow:airflow /opt/airflow
RUN rm -f $HADOOP_HOME/share/hadoop/common/lib/slf4j-log4j12-*.jar
RUN rm -f /opt/hive-3.1.3-bin/lib/log4j-slf4j-impl-2.17.1.jar
USER airflow

ENTRYPOINT ["/entrypoint.sh"]

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]


