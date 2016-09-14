package com.spark.streaming.tools.utils;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.io.Serializable;
import java.util.Properties;

public class ConnectionManager implements Serializable {

    /**
     * Properties are available here: http://kafka.apache.org/documentation.html#producerconfigs
     * Kafka Producer is thread safe - shared with all threads for best performance
     **/
    private static Producer stringProducer = null;
    private static Producer binaryProducer = null;

    private static Properties getBaseProducerKafkaProperties(StreamingConfig streamingConfig){
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("metadata.broker.list", streamingConfig.getProducerKafkaBrokers());
        kafkaProperties.put("batch.num.messages", streamingConfig.getProducerKafkaBatchSize());
        kafkaProperties.put("stringProducer.type", streamingConfig.getProducerKafkaType());
        kafkaProperties.put("topic.metadata.refresh.interval.ms", streamingConfig.getProducerMetadataRefreshInterval());
        kafkaProperties.put("compression.codec", "snappy");

        if (streamingConfig.getProducerAckEnabled().equalsIgnoreCase("1") ||
                streamingConfig.getProducerAckEnabled().equalsIgnoreCase("true")) {
            kafkaProperties.put("request.required.acks", "1");
        }

        return kafkaProperties;
    }

    public static Producer getSingletonConnectionWithStringEncoder(StreamingConfig streamingConfig) {
        if (null == stringProducer) {
            synchronized (ConnectionManager.class) {
                Properties kafkaProperties = getBaseProducerKafkaProperties(streamingConfig);
                kafkaProperties.put("serializer.class", "kafka.serializer.stringEncoder");
                ProducerConfig config = new ProducerConfig(kafkaProperties);
                stringProducer = new Producer<String, String>(config);
            }
        }

        return stringProducer;
    }

    public static Producer getKafkaSingletonConnectionWithBinaryEncoder(StreamingConfig streamingConfig) {
        if (null == binaryProducer) {
            synchronized (ConnectionManager.class) {
                Properties kafkaProperties = getBaseProducerKafkaProperties(streamingConfig);
                kafkaProperties.put("serializer.class", "kafka.serializer.DefaultEncoder");

                ProducerConfig config = new ProducerConfig(kafkaProperties);
                binaryProducer = new Producer<String, String>(config);
            }
        }

        return binaryProducer;
    }
}