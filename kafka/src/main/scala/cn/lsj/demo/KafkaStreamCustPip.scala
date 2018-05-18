package cn.lsj.demo

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized, Produced}
import org.codehaus.jettison.json.JSONObject

object KafkaStreamCustPip {

  def main(args: Array[String]) {
    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()
    val textLines: KStream[String, String] = builder.stream("user_events")

    textLines.print()
    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread { () => {
      streams.close(10, TimeUnit.SECONDS)
    }
    })
  }

}