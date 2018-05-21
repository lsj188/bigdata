package cn.lsj.demo

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json.JSONObject

import scala.util.Random


object KafkaEventProducer {
    def getKafkaProdecer(brokerList:String): KafkaProducer[String, String] = {
        val props = new Properties()
        props.put("client.id", "ScalaProducerExample")
        props.put("metadata.broker.list", brokerList)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList) //格式：host1:port1,host2:port2,....
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, new Integer(0)) //a batch size of zero will disable batching entirely
        props.put(ProducerConfig.LINGER_MS_CONFIG, new Integer(0)) //send message without delay
        props.put(ProducerConfig.ACKS_CONFIG, "1") //对应partition的leader写到本地后即返回成功。极端情况下，可能导致失败
        props.put("serializer.class", "kafka.serializer.StringEncoder")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        new KafkaProducer[String, String](props)
    }

    def send2Kafka(kafkaProducer: KafkaProducer[String, String], topic: String, lines: List[String]) = {
        for ( line <- lines){
            val record = new ProducerRecord[String,String](topic, line)
            println(record)
            kafkaProducer.send(record).get()
        }

    }


    private val users = Array(
        "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
        "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
        "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
        "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
        "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")
    //    private val users = Array("97edfc08311c70143401745a03a50706")

    private val random = new Random()
    private var pointer = -1

    def getUserID(): String = {
        pointer = pointer + 1
        if (pointer >= users.length) {
            pointer = 0
            users(pointer)
        } else {
            users(pointer)
        }
    }

    def click(): Double = {
        random.nextInt(10)
    }

    // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2
    // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
    // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
    // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning
    def main(args: Array[String]): Unit = {
        val topic = "user_events"
        //        val topic = "streams-plaintext-input"
        val brokers = "127.0.0.1:9092"
        val producer = getKafkaProdecer(brokers)

        while (true) {

            // prepare event data
            val event = new JSONObject()
            event
                .put("uid", getUserID)
                .put("event_time", System.currentTimeMillis.toString)
                .put("os_type", "Android")
                .put("click_count", click)


//            val line = getUserID + " " + " " + click + " " + System.currentTimeMillis.toString

            // produce event message
            send2Kafka(producer, topic, List[String](event.toString()))
            Thread.sleep(2000)

        }

    }

}
