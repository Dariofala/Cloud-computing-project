FROM python:3.10-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/local/bin/python3.10 /usr/bin/python3.10

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

ENV HADOOP_USER_NAME=hadoop

EXPOSE 8080

ENTRYPOINT ["python3"]
CMD ["-m", "swagger_server"]
