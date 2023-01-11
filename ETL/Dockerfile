FROM apache/airflow:latest-python3.9
COPY --chown=airflow:root /requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
