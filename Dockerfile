FROM python:3.12-bookworm
WORKDIR /app
COPY generador.py /app
RUN apt-get update && apt-get install nano -y
RUN pip install networkx