FROM apache/spark:3.5.3-python3

WORKDIR /opt/spark-apps
COPY jobs/bronze_writer.py /opt/spark-apps/bronze_writer.py

# Keep defaults; we'll pass --packages at runtime in docker-compose
ENV PYSPARK_PYTHON=python3
