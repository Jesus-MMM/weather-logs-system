FROM python:3.13

WORKDIR /app

RUN mkdir -p /app/logs && chmod 755 /app/logs

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "producer.py"]