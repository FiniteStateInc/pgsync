FROM python:3.9
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    jq cron \
  && rm -rf /var/lib/apt/lists/*
ARG WORKDIR=/code
RUN mkdir $WORKDIR
ADD ./examples/ $WORKDIR/examples
COPY . $WORKDIR/pgsync
RUN pip install -e $WORKDIR/pgsync
WORKDIR $WORKDIR
COPY ./docker/wait-for-it.sh wait-for-it.sh
COPY ./docker/runserver.sh runserver.sh
RUN chmod +x wait-for-it.sh
RUN chmod +x runserver.sh
