package cn.lsj.demo

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.KStream

object KafkaCustClient {
    def main(args: Array[String]) {

        val config: Properties = {
            val p = new Properties()
            p.put(StreamsConfig.APPLICATION_ID_CONFIG, "user_events_pip_cust1")
            p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
            p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
            p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
            p.put("group.id","pip_cust1")
            p
        }

        val builder: StreamsBuilder = new StreamsBuilder()
        val user_events_pip: KStream[String, String] = builder.stream("user_events_pip")
        user_events_pip.print()

        val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
        streams.start()

        Runtime.getRuntime.addShutdownHook(new Thread { () => {
            streams.close(10, TimeUnit.SECONDS)
        }
        })
    }
}
