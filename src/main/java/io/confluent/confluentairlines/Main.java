package io.confluent.confluentairlines;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Main {
    public static void resetKafka(Properties appProps) throws IOException {
        String cmd = String.format("kafka-consumer-groups --bootstrap-server %s --group %s --reset-offsets --to-earliest --all-topics --execute",
                appProps.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG),
                appProps.get(StreamsConfig.APPLICATION_ID_CONFIG)
        );

        final Executor exec = new DefaultExecutor();
        CommandLine cl = CommandLine.parse(cmd);
        if (exec.execute(cl) != 0) {
            throw new RuntimeException("failed to reset consumer");
        }
    }

    public static void main(String[] args) throws IOException {
        final String topic = "payments";
        final Properties appProps = new Properties();
        appProps.load(new FileInputStream("src/main/resources/kafka.properties"));

        End2EndSerializer end2EndSerializer = new End2EndSerializer();
        end2EndSerializer.setE2eAlgo(appProps.getProperty("e2e.algo"));
        end2EndSerializer.setE2eSecret(appProps.getProperty("e2e.secret"));

        End2EndDeserializer end2EndDeserializer = new End2EndDeserializer();
        end2EndDeserializer.setE2eAlgo(appProps.getProperty("e2e.algo"));
        end2EndDeserializer.setE2eSecret(appProps.getProperty("e2e.secret"));

        final Serde<JsonNode> e2eSerde = Serdes.serdeFrom(end2EndSerializer, end2EndDeserializer);


        // re-value stuffs
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(topic, Consumed.with(Serdes.String(), e2eSerde))
                // jsonNode for null record was encoded as JSON {null} so its not a true java null...
                .filter((paymentReference, jsonNode) -> jsonNode.toString().equals("null"))
                .map((key, value) -> new KeyValue<String, JsonNode>(key, null))
                .peek((k,v) -> System.out.println("Re-nulling " + k))
                .to(topic, Produced.with(Serdes.String(), e2eSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), appProps);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.cleanUp();
        resetKafka(appProps);
        System.out.println("====[starting stream fixer]====");
        kafkaStreams.start();

    }
}