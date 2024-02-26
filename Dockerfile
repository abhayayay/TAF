FROM python:3.10.3


RUN git config --global user.email "manik.x.mahajan@couchbase.com"
RUN git config --global user.name "manik-couchbase"
RUN touch temp_ini
RUN git clone https://github.com/couchbaselabs/TAF.git
WORKDIR /TAF
RUN git fetch
RUN git submodule init
RUN git submodule update --init --force --remote

COPY docker_taf.sh docker_taf.sh
RUN chmod 777 docker_taf.sh
ENTRYPOINT ["./docker_taf.sh"]