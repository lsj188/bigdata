package cn.lsj.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamCust {

    public static void main(final String[] args) throws Exception {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application-pip");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("user_events");
        textLines.print();
//        KTable<String, Integer> wordCounts = textLines.map((k, v) -> {
//            JSONObject json = null;
//            String kr = null;
//            Integer vr = null;
//            try {
//                json = new JSONObject(v);
//                kr = json.getString("uid");
//                vr = json.getInt("click_count");
//                System.out.println("source:"+kr+vr);
//            } catch (JSONException e) {
//                e.printStackTrace();
//            }
//            return KeyValue.pair(kr, vr);
//        }).groupByKey().reduce((a, b) -> a + b);
//        wordCounts.print();
//
////
////                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
////                .groupBy((key, word) -> word)
////                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
//        wordCounts.toStream().to("uid_click_count", Produced.with(Serdes.String(), Serdes.Integer()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

}