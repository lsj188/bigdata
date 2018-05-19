package cn.lsj.demo

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced}
import org.codehaus.jettison.json.JSONObject

import scala.collection.JavaConverters.asJavaIterableConverter


object KafkaStreamsCustPip {
    def main(args: Array[String]) {

        val config: Properties = {
            val p = new Properties()
            p.put(StreamsConfig.APPLICATION_ID_CONFIG, "user_events_pip_app")
            p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
            p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
            p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
            p
        }

        val builder: StreamsBuilder = new StreamsBuilder()
        val textLines: KStream[String, String] = builder.stream("user_events")
//        val wordCounts: KTable[String, Integer] = textLines.map { (k: String, v: String) => {
//            val json:JSONObject=null
//            try {
//                val json = new JSONObject(v)
//            }
//            catch {
//                case e: Exception => e.printStackTrace()
//            }
//            KeyValue.pair(json.getString("uid"), json.getInt("click_count"))
//        }
//        }.groupByKey().reduce(_ + _)
        textLines.print()
//        wordCounts.toStream().to("user_events_pip", Produced.`with`(Serdes.String(), Serdes.Integer()))

        val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
        streams.start()

        Runtime.getRuntime.addShutdownHook(new Thread { () => {
            streams.close(10, TimeUnit.SECONDS)
        }
        })
    }

}