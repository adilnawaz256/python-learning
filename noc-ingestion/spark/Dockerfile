FROM spark:3.5.1-python3

USER root

RUN mkdir -p /opt/spark/extra-jars /tmp/.ivy2 && \
    chmod -R 777 /tmp /opt/spark/extra-jars

# Copy vendored S3A and Iceberg runtime JARs into extra-jars


COPY spark/extra-jars/ /opt/spark/extra-jars/
COPY spark/entrypoint.sh /opt/spark/spark-apps/entrypoint.sh



RUN chmod +x /opt/spark/spark-apps/entrypoint.sh

USER 185
