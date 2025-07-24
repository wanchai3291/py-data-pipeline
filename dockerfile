FROM apache/airflow:3.0.3

# ใช้ USER airflow ตั้งแต่ต้น
USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
