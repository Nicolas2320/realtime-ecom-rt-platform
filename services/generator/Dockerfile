FROM python:3.11-slim

WORKDIR /app

COPY services/generator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/generator/app ./app

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "app.generator"]
