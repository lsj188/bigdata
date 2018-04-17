package cn.lsj.demo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


class WindowWC {
    def run(args: Map[String,String]) {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val host=args.get("host").fold("")(_.toString)
        val port=args.get("port").fold("")(_.toString).toInt
        val text = env.socketTextStream(host, port)

        val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
          .map { (_, 1) }
          .keyBy(0)
          .timeWindow(Time.seconds(5))
//          .sum(1)
        .fold("start")((str, i) => { str + "-" + i})  //合并同一key的value
        /** *
          * 3> start-(aa,1)
            3> start-(aa,1)-(aa,1)
            4> start-(cc,1)
            3> start-(aa,1)
            3> start-(bb,1)
          */

        counts.print()

        env.execute("Window Stream WordCount")
    }
}