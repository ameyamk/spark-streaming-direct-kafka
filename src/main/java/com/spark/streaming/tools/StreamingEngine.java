package com.spark.streaming.tools;


import com.google.common.base.Preconditions;
import com.spark.streaming.tools.core.AbstractSparkLayer;
import com.spark.streaming.tools.core.ProcessStreamingData;
import com.spark.streaming.tools.core.UpdateOffsetsFn;
import com.spark.streaming.tools.utils.CommandLineOptionsHandler;
import com.spark.streaming.tools.utils.StreamingConfig;
import kafka.message.MessageAndMetadata;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;

/*
 * Spark Streaming - Direct Kafka - Java Example Code
 * This code manages spark streaming managed using direct-kafka method.
 * Also manages offsets in Zoo keeper for better guarantees.
 * This is not exactly once - but rather "at least" once sort of a guarantee.
 * Which means this will produce duplicates - so you still need to manage this on
 * your own.
 *
 * Some performance considerations:
 *  - Make sure your avg batch processing time < your batch time
 *    If this is not the case. Reduce how much you read in your single batch.
 *    You can do this via this setting in streaming.yml file
 *
 *    - sparkStreamingKafkaMaxRatePerPartition
 *    - streamingBatchIntervalInSec
 *
 * Config File in Bundled in a JAR, you can also pass external config file if needed
 * Example Run command:
 *
 * $SPARK_HOME/bin/spark-submit --class com.spark.tools.streaming.StreamingEngine   \
 * --master yarn-cluster --queue public --num-executors 2 \
 * --driver-memory 6g  --executor-memory 6g --executor-cores 2 \
 * direct-streaming.jar
 *
 */
public class StreamingEngine extends AbstractSparkLayer {
    private static final Logger log = LoggerFactory.getLogger(StreamingEngine.class);

    private JavaStreamingContext streamingContext;

    public StreamingEngine(StreamingConfig config) {
        super.config = config;
    }

    public void start() {
        SparkConf sparkConf = getSparkConf();
        streamingContext = new JavaStreamingContext(sparkConf,
                Durations.seconds(Long.parseLong(config.getStreamingBatchIntervalInSec())));
        JavaInputDStream<MessageAndMetadata<String, byte[]>> dStream = buildInputDStream(streamingContext);
        JavaPairDStream<String, byte[]> pairDStream = dStream.mapToPair(km -> new Tuple2<>(km.key(), km.message()));

        pairDStream.foreachRDD(new ProcessStreamingData<>(config)); // process data
        dStream.foreachRDD(new UpdateOffsetsFn<>(config.getKafkaGroupId(), config.getZkOffsetManager()));
        streamingContext.start();
    }

    public void await() {
        Preconditions.checkState(streamingContext != null);
        log.info("Spark Streaming is running");
        streamingContext.awaitTermination();
    }

    @Override
    public synchronized void close() {
        if (streamingContext != null) {
            log.info("Shutting down Spark Streaming; this may take some time");
            streamingContext.stop(true, true);
            streamingContext = null;
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        StreamingConfig config = CommandLineOptionsHandler.getConfigs(args);
        StreamingEngine engine = new StreamingEngine(config);
        try {
            engine.start();
            engine.await();
        } finally {
            engine.close();
        }
    }
}