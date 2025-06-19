FROM jupyter/pyspark-notebook:python-3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop client
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz \
    && tar -xzf hadoop-3.2.1.tar.gz \
    && mv hadoop-3.2.1 /opt/hadoop \
    && rm hadoop-3.2.1.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

USER $NB_UID

# Copy requirements and install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /app

# Copy application code
COPY . /app/

# Expose ports
EXPOSE 8888 4040

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]