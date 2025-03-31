FROM python:3.11-bullseye

RUN apt-get update && apt-get upgrade --yes

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/bash", "entrypoint.sh"]




