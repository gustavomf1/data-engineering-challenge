FROM apache/airflow:2.8.0

COPY bibliotecas.txt .

RUN pip install --no-cache-dir --user -r bibliotecas.txt