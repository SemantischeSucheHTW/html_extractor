FROM python:3.7-stretch

RUN pip install kafka-python pymongo bs4

RUN mkdir /html_extractor
WORKDIR /html_extractor

COPY HTMLExtractor.py HTMLExtractor.py
COPY kafkasink.py kafkasink.py
COPY kafkasource.py kafkasource.py
COPY mongodbdao.py mongodbdao.py
COPY Order.py Order.py
COPY PageDetails.py PageDetails.py
COPY RawPageData.py RawPageData.py
COPY SoupArticle.py SoupArticle.py
COPY generateparseorders.py generateparseorders.py

COPY main.py main.py

ENV KAFKA_BOOTSTRAP_SERVERS kafka:9092
ENV KAFKA_PARSEORDERS_TOPIC parseorders
ENV KAFKA_PARSEORDERS_GROUP_ID html_extractor
ENV KAFKA_PAGEDETAILS_TOPIC pagedetails

ENV MONGODB_HOST mongo
ENV MONGODB_DB default
ENV MONGODB_RAWPAGES_COLLECTION rawpages
ENV MONGODB_PAGEDETAILS_COLLECTION pagedetails

ENV MONGODB_USERNAME genericparser
ENV MONGODB_PASSWORD genericparser

ENV DEBUG true

CMD ["python3", "-u", "main.py"]