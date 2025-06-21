FROM jupyter/pyspark-notebook:spark-3.1.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    openjdk-8-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Install compatible Hadoop version (3.2.1 to match Spark 3.1.1)
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz \
    && tar -xzf hadoop-3.2.1.tar.gz \
    && mv hadoop-3.2.1 /opt/hadoop \
    && rm hadoop-3.2.1.tar.gz

# Set Hadoop environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# Set Spark environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS=notebook

# Create Hadoop configuration directory and basic configs
RUN mkdir -p $HADOOP_CONF_DIR

# Create basic Hadoop configuration files
RUN echo '<?xml version="1.0" encoding="UTF-8"?>\n\
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\
<configuration>\n\
    <property>\n\
        <name>fs.defaultFS</name>\n\
        <value>hdfs://namenode:9000</value>\n\
    </property>\n\
    <property>\n\
        <name>hadoop.proxyuser.root.hosts</name>\n\
        <value>*</value>\n\
    </property>\n\
    <property>\n\
        <name>hadoop.proxyuser.root.groups</name>\n\
        <value>*</value>\n\
    </property>\n\
</configuration>' > $HADOOP_CONF_DIR/core-site.xml

RUN echo '<?xml version="1.0" encoding="UTF-8"?>\n\
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\
<configuration>\n\
    <property>\n\
        <name>dfs.webhdfs.enabled</name>\n\
        <value>true</value>\n\
    </property>\n\
    <property>\n\
        <name>dfs.permissions.enabled</name>\n\
        <value>false</value>\n\
    </property>\n\
</configuration>' > $HADOOP_CONF_DIR/hdfs-site.xml

USER $NB_UID

# Copy requirements and install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create necessary directories
RUN mkdir -p /home/$NB_USER/work/notebooks \
    && mkdir -p /home/$NB_USER/work/src \
    && mkdir -p /home/$NB_USER/work/data \
    && mkdir -p /home/$NB_USER/work/logs \
    && mkdir -p /home/$NB_USER/work/scripts

# Set working directory
WORKDIR /home/$NB_USER/work

# Expose ports
EXPOSE 8888 4040

# Create a startup script to configure Spark
RUN echo '#!/bin/bash\n\
export SPARK_MASTER_URL=${SPARK_MASTER:-spark://spark-master:7077}\n\
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-2g}\n\
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-2g}\n\
exec "$@"' > /home/$NB_USER/start-spark-notebook.sh \
    && chmod +x /home/$NB_USER/start-spark-notebook.sh

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''", "--NotebookApp.allow_root=True"]
