# Airflow image that can act as the Spark driver (client mode).
# Python 3.10 matches the Python in the Spark worker image.
FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
# Works on both amd64 and arm64 (Apple Silicon).
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
