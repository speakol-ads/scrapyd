FROM python:3.6.9-slim

RUN apt update && apt install htop nano vim procps -y
RUN pip install --upgrade pip && pip install \
    scrapy \
    redis \
    nltk \
    psycopg2-binary \
    requests \
    setuptools \
    sumy \
    nltk \
    pillow \
    sentry-sdk \
    cloudscraper \
    html2text \
    numpy \
    langdetect

WORKDIR /scrapyd

COPY . .

RUN python setup.py install

EXPOSE 6800

ENTRYPOINT ["scrapyd/scripts/scrapyd_run.py", "--pidfile="]

