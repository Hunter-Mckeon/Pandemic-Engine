# =============================================================================
# Dockerfile — Epidemic Engine Phase 2 Spark Analytics Job
#
# Packages the spark/etl.py job into a container image for deployment
# on OpenShift as a Kubernetes CronJob.
#
# Build and push to OpenShift internal registry:
#   docker build -t <registry>/ds551-2026-spring-7726b8/epidemic-analytics:latest .
#   docker push <registry>/ds551-2026-spring-7726b8/epidemic-analytics:latest
#
# Or using the OpenShift internal registry directly:
#   oc get route default-route -n openshift-image-registry --template='{{ .spec.host }}'
#   docker build -t <registry-route>/ds551-2026-spring-7726b8/epidemic-analytics:latest .
#   docker push <registry-route>/ds551-2026-spring-7726b8/epidemic-analytics:latest
# =============================================================================

FROM python:3.11-slim

# -----------------------------------------------------------------------------
# System dependencies — Java is required to run PySpark
# -----------------------------------------------------------------------------
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# -----------------------------------------------------------------------------
# Python dependencies
# -----------------------------------------------------------------------------
RUN pip install --no-cache-dir \
    pyspark==3.4.0 \
    psycopg2-binary==2.9.9 \
    kafka-python-ng==2.2.3

# -----------------------------------------------------------------------------
# Spark Kafka connector and PostgreSQL JDBC driver
# Downloaded once at build time so the container has no outbound dependency
# at runtime. Placed in the default Spark jars directory.
# -----------------------------------------------------------------------------
ENV SPARK_HOME=/usr/local/lib/python3.11/dist-packages/pyspark

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar" \
    -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar" \
    -o ${SPARK_HOME}/jars/postgresql-42.6.0.jar

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar" \
    -o ${SPARK_HOME}/jars/kafka-clients-3.3.2.jar

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar" \
    -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.4.0.jar

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
    -o ${SPARK_HOME}/jars/commons-pool2-2.11.1.jar

# -----------------------------------------------------------------------------
# Application code
# -----------------------------------------------------------------------------
WORKDIR /app
COPY spark/etl.py /app/etl.py

# -----------------------------------------------------------------------------
# Default command — runs the Spark analytics job in local mode
# For cluster-mode deployment, override this in the CronJob manifest
# -----------------------------------------------------------------------------
CMD ["python", "etl.py"]
