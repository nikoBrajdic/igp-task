FROM python:3.8.19-alpine3.20

LABEL authors="niko"

WORKDIR /code

COPY requirements.txt /code

RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

COPY . /code

ENTRYPOINT ["python", "numbers_parser.py"]
