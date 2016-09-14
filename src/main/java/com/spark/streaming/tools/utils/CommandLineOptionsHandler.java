package com.spark.streaming.tools.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CommandLineOptionsHandler {
    private static Logger log = LoggerFactory.getLogger(CommandLineOptionsHandler.class);
    private static final String KAFKA_TOPIC = "kafka-topic";
    private static final String REPARTITION_COUNT = "repartition-count";
    private static final String NAMESPACE = "namespace";
    private static final String BATCH_INTERVAL = "batch-interval";
    private static final String STREAMING_MAX_RATE_PER_PARTITION = "max-rate-partition";
    private static final String ENV = "env";

    public static StreamingConfig getConfigs(String[] args) throws ParseException, IOException {
        CommandLineParser parser = new GnuParser();
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt(KAFKA_TOPIC)
                .withDescription("Kafka topic name to consume from")
                .hasArg()
                .withArgName("Kafka topic name")
                .create());

        options.addOption(OptionBuilder.withLongOpt(REPARTITION_COUNT)
                .withDescription("Number of partitions rdd should be split into. This helps to increase parallelism")
                .hasArg()
                .withArgName("RDD re-partitions count")
                .create());

        options.addOption(OptionBuilder.withLongOpt(NAMESPACE)
                .withDescription("Namespace for the job, also used as a way to checkpoint")
                .hasArg()
                .withArgName("Namespace")
                .create());

        options.addOption(OptionBuilder.withLongOpt(STREAMING_MAX_RATE_PER_PARTITION)
                .withDescription("Max Number of messages to read per partition per batch interval")
                .hasArg()
                .withArgName("Max messages read per kafka partition")
                .create());

        options.addOption(OptionBuilder.withLongOpt(BATCH_INTERVAL)
                .withDescription("Spark streaming batch interval")
                .hasArg()
                .withArgName("Batch  interval")
                .create());

        options.addOption(OptionBuilder.withLongOpt(ENV)
                .withDescription("Configuration environment to use")
                .hasArg()
                .withArgName("Configuration environment. default is staging")
                .create());

        CommandLine line = parser.parse(options, args);

        StreamingConfig config = ConfigLoader.getDefaultConfigs();

        if (line.hasOption(REPARTITION_COUNT)) {
            config.setRepartitionCount(line.getOptionValue(REPARTITION_COUNT));
        }

        if (line.hasOption(NAMESPACE)) {
            config.setNamespace(line.getOptionValue(NAMESPACE));
        }

        if (line.hasOption(STREAMING_MAX_RATE_PER_PARTITION)) {
            config.setSparkStreamingKafkaMaxRatePerPartition(line.getOptionValue(STREAMING_MAX_RATE_PER_PARTITION));
        }

        if (line.hasOption(BATCH_INTERVAL)) {
            config.setStreamingBatchIntervalInSec(line.getOptionValue(BATCH_INTERVAL));
        }

        log.info("Starting with Spark Engine with configs  " + config);
        return config;
    }
}