FROM sequenceiq/hadoop-docker:2.7.1

ADD . /usr/local/hadoop/lib/new_native
RUN rm -r /usr/local/hadoop/lib/native
RUN mv /usr/local/hadoop/lib/new_native /usr/local/hadoop/lib/native