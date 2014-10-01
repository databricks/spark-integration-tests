FROM spark-test-base

RUN wget -q https://archive.apache.org/dist/kafka/0.8.0/kafka_2.8.0-0.8.0.tar.gz -O /tmp/kafka.tgz
RUN mkdir /opt/kafka
RUN tar xfz /tmp/kafka.tgz -C /opt/kafka --strip-components=1

ENV KAFKA_HOME /opt/kafka

# Set the maximum message size to 2MB.
RUN echo 'message.max.bytes=2000000' >> $KAFKA_HOME/config/server.properties
RUN echo 'replica.fetch.max.bytes=2000000' >> $KAFKA_HOME/config/server.properties

ADD start-kafka.sh /usr/bin/start-kafka.sh
CMD start-kafka.sh
