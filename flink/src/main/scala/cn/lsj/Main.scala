package cn.lsj

import cn.lsj.demo.{TableAndSql, WindowWC}

/** *
  * ./bin/flink run examples/streaming/SocketWindowWordCount.jar WindowWC host=localhost;port=9999
  * 入口主类
  */
object Main {
    /**
      *
      * @param args 类名 key1=value1;key2=value2;key3=value3
      */
    def main(args: Array[String]): Unit = {
        //获取执行类名
        val mainClass: String = try {
            args(0)
        } catch {
            case e: Exception => {
                System.err.println("Input args error!'")
                return
            }
        }
        //处理参数列表
        val argsMap: Map[String, String] = try {
            args(1).split(";").map { s => val a = s.split("="); (a(0), a(1)) }.toMap
        } catch {
            case e: Exception => {
                System.err.println("Input args is null!'")
                Map[String, String]()
            }
        }


        /**
          *
          * @param name 根据传入的类名选择执行类
          *
          */
        def run(name: String) {
            name match {
                //从前往后匹配
                case "WindowWC" => new WindowWC().run(argsMap)
                case "TableAndSql" => new TableAndSql().csvTabSource(argsMap)
                case _ => println("找不到匹配的类！")

            }
        }

        //输出参数
        println("mainClass="+mainClass+"\nargMap="+argsMap)

        //执行类
        run(mainClass)
    }

}