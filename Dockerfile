FROM debian:10

LABEL maintainer="dmintz"

RUN apt-get update --fix-missing
RUN apt-get install -y python3 python3-dev python3-pip nginx
RUN pip3 install uwsgi

# We copy just the requirements.txt first to leverage Docker cache
COPY ./requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

COPY ./ /app
WORKDIR /app

RUN mkdir -p /app/logs


EXPOSE 80
CMD [ "uwsgi", "--ini", "/app/Omni.ini" ]