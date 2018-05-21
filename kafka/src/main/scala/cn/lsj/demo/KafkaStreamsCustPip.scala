package cn.lsj.demo

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.codehaus.jettison.json.JSONObject

object KafkaStreamsCustPip {
    def main(args: Array[String]) {

        val config: Properties = {
            val p = new Properties()
            p.put(StreamsConfig.APPLICATION_ID_CONFIG, "user_events_pip_app")
            p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
            p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
            p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
            p
        }

        val builder: StreamsBuilder = new StreamsBuilder()
        val textLines: KStream[String, String] = builder.stream("user_events")
        textLines.print()
        val wordCounts: KTable[String, String] = textLines.map[String, String] {
            //kafka有些不支持scala语法，需要按JAVA来写，注意看编译错误信息
            new KeyValueMapper[String, String, KeyValue[String, String]] {
                override def apply(k: String, v: String): KeyValue[String, String] = {
                    val json: JSONObject = new JSONObject(v)
                    return KeyValue.pair(json.getString("uid"), json.getString("click_count"))
                }
            }
        }.groupByKey().reduce(new Reducer[String] {
            //kafka有些不支持scala语法，需要按JAVA来写，注意看编译错误信息
            override def apply(aggValue: String, newValue: String): String = (aggValue.toLong+newValue.toLong).toString
        })

        wordCounts.toStream().to("user_events_pip")

        val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
        streams.start()

        Runtime.getRuntime.addShutdownHook(new Thread { () => {
            streams.close(10, TimeUnit.SECONDS)
        }
        })
    }

}