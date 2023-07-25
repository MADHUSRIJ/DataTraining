#!/bin/bash

# Build the base images from which are based the Dockerfiles
# then Startup all the containers at once 
docker build -t hadoop-base docker/hadoop/hadoop-base && \
docker build -t hive-base docker/hive/hive-base && \
docker build -t spark-base docker/spark/spark-base && \
docker build -t airflow-base docker/airflow && \
docker build -t hue-base docker/hue && \
docker build -t livy-base docker/livy && \
docker build -t postgres-base docker/postgres & \
docker-compose up -d --build
