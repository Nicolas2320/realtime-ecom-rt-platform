FROM apache/spark:3.5.3-python3

USER root
RUN pip install --no-cache-dir "psycopg[binary]==3.2.3"
USER spark

WORKDIR /opt/spark-apps
COPY jobs/gold_kpi_writer.py /opt/spark-apps/gold_kpi_writer.py
