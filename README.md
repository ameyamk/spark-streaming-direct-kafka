# spark-streaming-direct-kafka
High Performance Spark Streaming with Direct Kafka in Java

## What & Why

Simple library provides easy way to consume from Kafka using Spark Streaming. This lib keeps offsets in zookeeper - instead of them stored in HDFS. Since lib stores offsets only once per batch - we can achieve very high throughput.

This is relatively reliable - but there can be still some data loss. But in most scenarios this provide at least once guarantees.
We managed to consume over 100,000 messages/ sec using this lib.

## How to Run:

This is how you start your job:
spark-streaming-direct-kafka/src/main/java/com/spark/streaming/tools/StreamingEngine.java

Configs are self explanatory and can be changed here:
spark-streaming-direct-kafka/src/main/resources/streaming.yml



