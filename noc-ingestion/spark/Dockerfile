FROM spark:3.5.1-python3

USER root

# Create writable HOME directory for spark user (UID 185) and configure Ivy cache directory
RUN mkdir -p /home/spark/.ivy2/cache /tmp/.ivy2 /opt/spark/work-dir && \
    chown -R 185:0 /home/spark /tmp/.ivy2 /opt/spark/work-dir && \
    chmod -R 777 /home/spark /tmp/.ivy2 /opt/spark/work-dir

ENV HOME=/home/spark

USER 185
