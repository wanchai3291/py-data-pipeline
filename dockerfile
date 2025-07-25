FROM apache/airflow:3.0.3

USER airflow

COPY requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
