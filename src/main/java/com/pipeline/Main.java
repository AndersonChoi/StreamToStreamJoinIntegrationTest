package com.pipeline;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private final static Logger log = LoggerFactory.getLogger(Main.class);
    private static final String adImpressionsTopic = "adImpressions";
    private static final String adClicksTopic = "adClicks";
    private static final String outputTopic = "test";

    public static void main(String[] args) {
        log.info("start");
        Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-join-lambda-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> impressionStream = builder.stream(adImpressionsTopic);
        KStream<String, String> clickStream = builder.stream(adClicksTopic);

        // In this example, we opt to perform an OUTER JOIN between the two streams.  We picked this
        // join type to show how the Streams API will send further join updates downstream whenever,
        // for the same join key (e.g. "newspaper-advertisement"), we receive an update from either of
        // the two joined streams during the defined join window.
        final KStream<String, String> impressionsAndClicks = impressionStream.outerJoin(
                clickStream,
                (impressionValue, clickValue) -> getClickedImpression(impressionValue, clickValue),
                JoinWindows.of(Duration.ofSeconds(5)));

        // Write the results to the output topic.
        impressionsAndClicks.to(outputTopic);

        KafkaStreams streams;
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        // 토폴로지 그래프 확인
        final Topology topology = builder.build();
        System.out.println(topology.describe());


    }

    private static String getClickedImpression(String impressionValue, String clickValue) {
        log.info(impressionValue + "|||" + clickValue);

        if (clickValue == null) {
            return impressionValue + "/not-clicked-yet";
        } else {
            return impressionValue + "/" + clickValue;
        }
    }


}
