FROM apache/airflow:2.7.1-python3.9

USER airflow

COPY requirements.txt .
COPY .env .
COPY utils.py .
COPY credentials.json .

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt