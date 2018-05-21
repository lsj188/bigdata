package cn.lsj.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class KafkaStreamCust {

    public static void main(final String[] args) throws Exception {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application-pip");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 指定一个路径创建改应用ID所属的文件
        config.put(StreamsConfig.STATE_DIR_CONFIG, "E:\\bigdata_install\\kafka_2.11-0.11.0.1\\kafka-stream");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("user_events");
        textLines.print();
        KTable<String, Long> wordCounts = textLines.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "))
        ).groupBy((k, v) -> v)
                .count();
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
        System.out.println("KafkaStreamCust:==========================================================");
        wordCounts.print();
//
////
////                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
////                .groupBy((key, word) -> word)
////                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
//        wordCounts.toStream().to("uid_click_count", Produced.with(Serdes.String(), Serdes.Integer()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}