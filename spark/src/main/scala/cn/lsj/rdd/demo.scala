package cn.lsj.rdd

import java.io.IOException

import cn.lsj.comm.SparkInit
import cn.lsj.tools.FileTools
import org.apache.spark.storage.StorageLevel

object Main {
    def main(args: Array[String]): Unit = {
        //    if (args(0) == "CoursesSum") {
        //      val cour: CoursesSum = new CoursesSum()
        //      cour.run(args)
        //    }
        val cour = new CoursesSum()
        cour.run()
    }
}


class CoursesSum() extends Serializable {
    /**
     * | Field        | Type          | Null | Key | Default | Extra |
     * +--------------+---------------+------+-----+---------+-------+
     * | type         | varchar(100)  | YES  |     | NULL    |       |
     * | level        | varchar(20)   | YES  |     | NULL    |       |
     * | title        | varchar(200)  | YES  |     | NULL    |       |
     * | url          | varchar(300)  | YES  |     | NULL    |       |
     * | image_path   | varchar(300)  | YES  |     | NULL    |       |
     * | image_url    | varchar(300)  | YES  |     | NULL    |       |
     * | student      | varchar(10)   | YES  |     | NULL    |       |
     * | introduction | varchar(3000) | YES  |     | NULL    |       |
     * | in_time      | datetime      | YES  |     | NULL    |       |
+--------------+---------------+------+-----+---------+-------+
     **/
    def run(): Unit = {
        //打包执行（spark-submit）时，默认使用HDFS文件系统，如需使用本地文件需使用file前缀，output文件不能是file
        val inFiles = List("file:///E:\\git\\my-spark\\testData\\test.courses")
        val outFiles = List("/tmp/spark/test.courses.sum1",
            "/tmp/spark/test.courses.sum2",
            "/tmp/spark/test.courses.join1",
            "/tmp/spark/test.courses.leftjoin",
            "/tmp/spark/test.courses.rightjoin",
            "/tmp/spark/test.courses.mapjoin"
        )
        val (sc, sqlContext) = SparkInit.initSpark("local", "coureseSum")


        // 删除目标路径
        try {
            for (path <- outFiles)
                FileTools.delPath(path)
        } catch {
            case ex: IOException => ex.printStackTrace()
        }


        //        val coureseRdd = sc.textFile(inFiles(0) + "\\*").map(_.split(","))
        //wholeTextFiles方法生成的RDD为（K，V）,K为文件名路径，V为整个文件的内容，当小文件多时可以使用
        val coureseRdd = sc.wholeTextFiles(inFiles(0) + "\\*")
          .flatMap((file) => for (i <- file._2.split("\n")) yield i.split(","))


        //缓存RDD数据
//        coureseRdd.persist(StorageLevel.MEMORY_AND_DISK)


        //按类型和级别统计人数
        val students = coureseRdd
          .map((cols: Array[String]) => ((cols(0), cols(1)), (cols(6).toLong,1)))
          .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
          .coalesce(10) //缩减文件数
          .sortBy((x) => (x._1._1, x._2._1), false) //降序排序
          .map((s) => s._1._1 + "," + s._1._2 + "," + s._2._1+","+s._2._2).coalesce(1) //处理文件输出格式
        students.take(20).foreach(println) //取前20条数据打印


        //缓存RDD数据
        students.persist(StorageLevel.MEMORY_AND_DISK)
        //        students.saveAsTextFile(outFiles(0))


        //按级别统计人数及课程门数
        val levelStudents = students
          .map((row) => {val cols=row.split(",");(cols(1), (cols(2).toLong, cols(3).toLong))}) //重新构建K，V()
          //          .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
          //当输入类型与输出类型不同时可使用aggregateByKey减少创建对象的开销
          .aggregateByKey((0L, 0L))((a, b) => (a._1 + b._1, a._2 + b._2), (a, b) => (a._1 + b._1, a._2 + b._2))
          .map((row) => List(row._1, row._2._1, row._2._2).mkString(",")).coalesce(1) // 处理文件输入格式
        //        levelStudents.saveAsTextFile(outFiles(1))

        val leftRdd = students.map((line) => {
            val cols = line.split(",")
            (cols(1), cols)
        }) //处理成可关联的K，V
        //        leftRdd.saveAsTextFile(outFiles(3))

        val rightRdd = levelStudents.union(sc.parallelize(Seq(List("未知", "123", "456").mkString(","), List("未知1", "1231", "4561").mkString(",")))).map((line) => {
            val cols = line.split(",")
            (cols(0), cols)
        }) //处理成可关联的K，V
        //        leftRdd.saveAsTextFile(outFiles(4))


        //==================join===================
        val innerjoinRdd = leftRdd
          .join(rightRdd) //处理成可关联的K，V
          .map((obj) => obj._2._1.mkString(",") + "," + obj._2._2.mkString(",")).coalesce(1) //拼接关联结果
        innerjoinRdd.saveAsTextFile(outFiles(2))
        //
        //
        //        val leftJoinRdd = leftRdd.leftOuterJoin(rightRdd).map((obj) => obj._2._1.mkString(",") + "," + (obj._2._2 match {
        //            case Some(a) => a.mkString(",")
        //            case None => "No this Skill"
        //        })).coalesce(1)
        ////        leftJoinRdd.saveAsTextFile(outFiles(3))
        //
        //
        //        val rightJoinRdd = leftRdd.rightOuterJoin(rightRdd).map((obj) => (obj._2._1 match {
        //            case Some(a) => a.mkString(",")
        //            case None => "No this Skill"
        //        }) + "," + obj._2._2.mkString(",")).coalesce(1)
        //        rightJoinRdd.saveAsTextFile(outFiles(4))
        //==================join===================


        //================mapjoin==================
        import scala.collection.mutable.Map

        //提取RDD数据放入Map中
        val levelSumMap = Map[String, String]()
        for (i <- rightRdd.collect())
            levelSumMap += (i._1 -> i._2.mkString(","))

        //定义广播变量
        val bcLevel = sc.broadcast(levelSumMap)

        //map端join
        val mapJoin = leftRdd.map((obj) => {
            val levelLine = bcLevel.value.get(obj._1) //获取广播变量数据并
            obj._2.mkString(",") + "," + (levelLine match {
                case Some(a) => a
                case None => "No match"
            })
        }).coalesce(1)
        mapJoin.saveAsTextFile(outFiles(5))

        //================mapjoin==================




        Thread.sleep(10000000)
        sc.stop()
    }


}

//class IeteFlowSum() {
//
//}
