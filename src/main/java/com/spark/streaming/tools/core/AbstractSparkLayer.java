package com.spark.streaming.tools.core;

import com.spark.streaming.tools.utils.StreamingConfig;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractSparkLayer implements Closeable {

    protected StreamingConfig config;

    public SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition",
                config.getSparkStreamingKafkaMaxRatePerPartition()); // rate limiting
        sparkConf.setAppName("StreamingEngine-" + config.getTopicSet().toString() + "-" + config.getNamespace());

        if (config.getLocalMode()) {
            sparkConf.setMaster("local[4]");
        }
        return sparkConf;
    }

    public JavaInputDStream<MessageAndMetadata<String,byte[]>> buildInputDStream(
            JavaStreamingContext streamingContext) {

        HashMap<String, String> kafkaParams = config.getKafkaParams();

        // Ugly compiler-pleasing acrobatics:
        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, byte[]>> streamClass =
                (Class<MessageAndMetadata<String, byte[]>>) (Class<?>) MessageAndMetadata.class;

        if (!KafkaManager.topicExists(config.getZkKafka(), config.getTopic())) {
            throw new RuntimeException("Topic does not exist on server");
        }

        Map<TopicAndPartition, Long> seedOffsetsMap = KafkaManager.getOffsets(config.getZkKafka(),
                config.getZkOffsetManager(), config.getKafkaGroupId(), config.getTopic(), config.getKafkaParams());

        // TODO: try generics, instead of hardcoded values
        JavaInputDStream<MessageAndMetadata<String, byte[]>> dStream = org.apache.spark.streaming.kafka.KafkaUtils.createDirectStream(
                streamingContext,
                String.class,  // change as necessary
                byte[].class,  // change as necessary
                StringDecoder.class,
                DefaultDecoder.class,
                streamClass,
                kafkaParams,
                seedOffsetsMap,
                Functions.<MessageAndMetadata<String, byte[]>>identity());
        return dStream;
    }
}