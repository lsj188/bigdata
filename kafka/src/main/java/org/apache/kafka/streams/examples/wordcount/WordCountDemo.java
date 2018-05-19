//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.kafka.streams.examples.wordcount;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class WordCountDemo {
    public WordCountDemo() {
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("application.id", "streams-wordcount");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("cache.max.bytes.buffering", 0);
        props.put("default.key.serde", Serdes.String().getClass().getName());
        props.put("default.value.serde", Serdes.String().getClass().getName());
        props.put("auto.offset.reset", "earliest");
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream("user_events");
        KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
            }
        }).groupBy(new KeyValueMapper<String, String, String>() {
            public String apply(String key, String value) {
                return value;
            }
        }).count("Counts");
        counts.to(Serdes.String(), Serdes.Long(), "streams-wordcount-output");
        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable var8) {
            System.exit(1);
        }

        System.exit(0);
    }
}
