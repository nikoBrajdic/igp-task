FROM python:3.8.19-alpine3.20

LABEL authors="niko"

WORKDIR /code

COPY . /code

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:8000", "main:app"]
