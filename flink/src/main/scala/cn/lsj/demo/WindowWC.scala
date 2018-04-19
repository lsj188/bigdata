package cn.lsj.demo

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


class WindowWC {
    def run(args: Map[String,String]) {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
            3, // 每个测量时间间隔最大失败次数
            org.apache.flink.api.common.time.Time.seconds(5),// 每个测量时间间隔最大失败次数
            org.apache.flink.api.common.time.Time.seconds(10)) // 两次连续重启尝试的时间间隔
        )


        val host=args.get("host").fold("")(_.toString)
        val port=args.get("port").fold("")(_.toString).toInt
        val text = env.socketTextStream(host, port)

        val counts = text.flatMap{_.toLowerCase.split("\\W+") filter { _.nonEmpty } }
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