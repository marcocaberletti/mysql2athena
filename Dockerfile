FROM python:3.11.2-slim

RUN apt-get update && \
    apt-get -y install gcc default-libmysqlclient-dev && \
    apt-get clean all

RUN useradd --system -m runner
WORKDIR /home/runner

ADD requirements.txt .
RUN pip3 install -r requirements.txt
ADD . .

USER runner
ENTRYPOINT [ "./mysql2athena.py" ]
