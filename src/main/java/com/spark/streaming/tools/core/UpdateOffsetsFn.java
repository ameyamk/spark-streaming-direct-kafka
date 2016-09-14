package com.spark.streaming.tools.core;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Function that reads offset range from latest RDD in a streaming job, and updates
 * Zookeeper/Kafka with the latest offset consumed.
 *
 * @param <K> RDD element's key type (not used)
 * @param <M> RDD element's value type (not used)
 */
public final class UpdateOffsetsFn<K,M> implements Function<JavaRDD<MessageAndMetadata<K,M>>,Void> {

    private static final Logger log = LoggerFactory.getLogger(UpdateOffsetsFn.class);

    private final String group;
    private final String zkOffsetManager;

    public UpdateOffsetsFn(String group, String zkOffsetManager) {
        this.group = group;
        this.zkOffsetManager = zkOffsetManager;
    }

    /**
     * @param javaRDD RDD whose underlying RDD must be an instance of {@link HasOffsetRanges},
     *  such as {@code KafkaRDD}
     * @return null
     */
    @Override
    public Void call(JavaRDD<MessageAndMetadata<K,M>> javaRDD) {
        OffsetRange[] ranges = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();
        Map<TopicAndPartition,Long> newOffsets = new HashMap<>(ranges.length);
        for (OffsetRange range : ranges) {
            newOffsets.put(new TopicAndPartition(range.topic(), range.partition()),
                    range.untilOffset());
        }
        log.info("Updating offsets: {}", newOffsets);
        KafkaManager.setOffsets(zkOffsetManager, group, newOffsets);
        return null;
    }

}