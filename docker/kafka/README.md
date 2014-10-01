This is based on https://github.com/hwasungmars/kafka-broker-docker

The main changes are:

   - use Kafka 0.8.0 instead of 0.8.1.1.
   - set `host.name` instead of `advertised.host.name`.
   - build on our own Spark base image.
