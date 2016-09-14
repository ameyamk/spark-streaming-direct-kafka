package com.spark.streaming.tools.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Configs for Spark Streaming app
 * Additional documentation about Kafka Properties are available here: http://kafka.apache.org/documentation.html#producerconfigs
 **/
public class StreamingConfig implements Serializable {

    public String sparkStreamingKafkaMaxRatePerPartition;
    public String repartitionCount;

    public String streamingBatchIntervalInSec;
    public String namespace;
    public String checkpointDir;

    public String kafkaTopics;

    public String kafkaBrokers;
    public String kafkaAutoOffsetReset;
    public String kafkaMaxFetchBytes;
    public String kafkaGroupId;

    public String zkKafka;
    public String zkOffsetManager;

    public String producerKafkaTopic;
    public String producerKafkaErrorMessagesTopic;
    public String producerKafkaBrokers;
    public String producerKafkaType;
    public String producerAckEnabled;
    public String producerKafkaBatchSize;
    public String producerMetadataRefreshInterval;
    public String producerKafkaTopicPrefix;

    public String localMode;

    HashMap<String, String> kafkaParams;

    public String getSparkStreamingKafkaMaxRatePerPartition() {
        return sparkStreamingKafkaMaxRatePerPartition;
    }

    public void setSparkStreamingKafkaMaxRatePerPartition(String sparkStreamingKafkaMaxRatePerPartition) {
        this.sparkStreamingKafkaMaxRatePerPartition = sparkStreamingKafkaMaxRatePerPartition;
    }

    public int getRepartitionCount() {
        if (getLocalMode())
            return 1;

        return Integer.parseInt(repartitionCount);
    }

    public void setRepartitionCount(String repartitionCount) {
        this.repartitionCount = repartitionCount;
    }

    public String getStreamingBatchIntervalInSec() {
        return streamingBatchIntervalInSec;
    }

    public void setStreamingBatchIntervalInSec(String streamingBatchIntervalInSec) {
        this.streamingBatchIntervalInSec = streamingBatchIntervalInSec;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getKafkaBrokers() {
        return kafkaBrokers;
    }

    public String getKafkaAutoOffsetReset() {
        return kafkaAutoOffsetReset;
    }

    public String getKafkaMaxFetchBytes() {
        return kafkaMaxFetchBytes;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getZkKafka() {
        return zkKafka;
    }

    public String getZkOffsetManager() {
        return zkOffsetManager;
    }

    public HashMap<String, String> getKafkaParams() {
        kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", getKafkaBrokers());
        kafkaParams.put("auto.offset.reset", getKafkaAutoOffsetReset());
        kafkaParams.put("group.id", getKafkaGroupId());
        kafkaParams.put("client.id", getNamespace());
        kafkaParams.put("fetch.message.max.bytes", getKafkaMaxFetchBytes());
        return kafkaParams;
    }

    public boolean getLocalMode() {
        if (localMode.equalsIgnoreCase("true"))
            return true;

        return false;
    }

    public String getDefaultProducerKafkaTopicName() {
        return producerKafkaTopicPrefix + producerKafkaTopic;
    }

    public String getProducerKafkaErrorMessagesTopicName() {
        return producerKafkaTopicPrefix + producerKafkaErrorMessagesTopic;
    }

    public String getProducerKafkaBrokers() {
        return producerKafkaBrokers;
    }

    public String getProducerKafkaType() {
        if (producerKafkaType == null)
            return "sync";

        return producerKafkaType;
    }

    public String getProducerAckEnabled() {
        if (producerAckEnabled == null)
            return "false";

        return producerAckEnabled;
    }

    public String getProducerKafkaBatchSize() {
        if(producerKafkaBatchSize == null)
            return "200";

        return producerKafkaBatchSize;
    }

    public String getProducerMetadataRefreshInterval() {
        if (producerMetadataRefreshInterval == null)
            return "30000";
        return producerMetadataRefreshInterval;
    }

    public void setKafkaTopics(String kafkaTopics) {
        this.kafkaTopics = kafkaTopics;
    }

    public Set<String> getTopicSet() {
        Set<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopics.split(",")));
        return topicsSet;
    }

    public String getTopic(){
        return  kafkaTopics;
    }

    @Override
    public String toString() {
        return "StreamingConfig{" +
                "sparkStreamingKafkaMaxRatePerPartition='" + sparkStreamingKafkaMaxRatePerPartition + '\'' +
                ", streamingBatchIntervalInSec='" + streamingBatchIntervalInSec + '\'' +
                ", namespace='" + namespace + '\'' +
                ", checkpointDir='" + checkpointDir + '\'' +
                ", kafkaTopics='" + kafkaTopics + '\'' +
                ", kafkaBrokers='" + kafkaBrokers + '\'' +
                ", kafkaAutoOffsetReset='" + kafkaAutoOffsetReset + '\'' +
                ", kafkaMaxFetchBytes='" + kafkaMaxFetchBytes + '\'' +
                ", producerKafkaTopic='" + producerKafkaTopic + '\'' +
                ", producerKafkaBrokers='" + producerKafkaBrokers + '\'' +
                ", producerKafkaType='" + producerKafkaType + '\'' +
                ", producerAckEnabled='" + producerAckEnabled + '\'' +
                ", producerKafkaBatchSize='" + producerKafkaBatchSize + '\'' +
                ", localMode='" + localMode + '\'' +
                '}';
    }
}