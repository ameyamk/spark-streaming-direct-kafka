package com.spark.streaming.tools.core;

import com.google.common.collect.Lists;
import com.spark.streaming.tools.utils.ConnectionManager;
import com.spark.streaming.tools.utils.StreamingConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/*
 * Write code here that does processes streaming data
 */
public class ProcessStreamingData<K, M, U> implements Serializable, Function<JavaPairRDD<K, M>, Void> {
    private static final Logger logger = LoggerFactory.getLogger(ProcessStreamingData.class);

    private final StreamingConfig config;

    public ProcessStreamingData(StreamingConfig config) {
        this.config = config;
    }

    /*
     * This is how it works
     * 1. Each RDD is split into multiple partitions for better parallel processing
     * 2. Each tuple in in each partition is processed.
     * 3. Output of this is sent to kafka from its own partition (and that's why for each partition logic)
     *
     */
    public void execute(JavaPairRDD<String, byte[]> inputMessage) {
        JavaPairRDD<String, byte[]> partitionedRDD;
        if (config.getLocalMode())
            partitionedRDD = inputMessage;
        else {
            // Helps scale beyond number of input partitions in kafka
            partitionedRDD = inputMessage.repartition(config.getRepartitionCount());

        }

        partitionedRDD.foreachPartition(prdd -> {
            // You can choose binary or string encoder
            Producer validProducer = ConnectionManager.getKafkaSingletonConnectionWithBinaryEncoder(config);
            prdd.forEachRemaining(records -> {
                byte[] msg = records._2();
                try {
                    // TODO: Add your logic here to process data
                    // As default we are just publishing back to another kafka topic
                    logger.info("Processing event=" + new String(msg));
                    publishMessagesToKafka(validProducer, msg);
                } catch (Exception e){
                    logger.error("Error processing message:" + msg);
                }
            });
        });
    }

    public void publishMessagesToKafka(Producer producer, byte[] message) {
        try {
            List<KeyedMessage<String, byte[]>> keyedMessageList = Lists.newArrayListWithCapacity(1);
            String topic = config.getDefaultProducerKafkaTopicName();
            keyedMessageList.add(new KeyedMessage<>(topic, message));
            producer.send(keyedMessageList);
        } catch (Exception e) {
            logger.error("Error occurred while publishing to error kafka queue {}", e);
        }
    }

    @Override
    public Void call(JavaPairRDD<K, M> kmJavaPairRDD) throws Exception {
        execute((JavaPairRDD<String, byte[]>) kmJavaPairRDD);
        return null;
    }
}