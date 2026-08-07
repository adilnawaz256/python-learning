FROM spark:3.5.1-python3

USER root

RUN mkdir -p /opt/spark/extra-jars /tmp/.ivy2 && \
    chmod -R 777 /tmp /opt/spark/extra-jars

# Copy vendored S3A and Iceberg runtime JARs into extra-jars
COPY extra-jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar
COPY extra-jars/hadoop-aws-3.3.4.jar /opt/spark/extra-jars/hadoop-aws-3.3.4.jar
COPY extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar /opt/spark/extra-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar

COPY entrypoint.sh /opt/spark/spark-apps/entrypoint.sh
RUN chmod +x /opt/spark/spark-apps/entrypoint.sh

USER 185
