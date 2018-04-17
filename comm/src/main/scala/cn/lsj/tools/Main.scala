package cn.lsj.tools

/** *
  * ./bin/flink run examples/streaming/SocketWindowWordCount.jar --main-class flink-WindowWC  --args host=localhost;port=9999
  */
object Main {
    /** *
      *
      * @param args
      *
      */
    def main(args: Array[String]): Unit = {
        //执行类的名字
        val mainClass: String = try {
            args(0)
        } catch {
            case e: Exception => {
                System.err.println("Input args error!'")
                return
            }
        }
        //参数列表
        val hMap: Map[String, String] = try {
            args(1).split(";").map { s => val a = s.split("="); (a(0), a(1)) }.toMap
        } catch {
            case e: Exception => {
                System.err.println("Input args error!'")
                Map[String, String]()
            }
        }


        if (args {
            0
        }.equals("flink-WindowWC")) {
            //        new WindowWC().run(hMap);
        }
    }

}
