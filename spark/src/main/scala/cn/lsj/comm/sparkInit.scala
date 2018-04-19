package cn.lsj.comm

import java.io.IOException

import cn.lsj.tools.FileTools
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SparkInit {
    /**
      * 初使化spark
      **/
    def initSpark(masterUrl: String, appName: String): (SparkContext, SQLContext) = {
        //    val conf = new SparkConf().setMaster("yarn-client").setAppName("Test")
        val conf = new SparkConf().setMaster(masterUrl).setAppName(appName)
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        (sc, sqlContext)
    }

    /**
      * 删除存在的目录
      **/
    def delFilePath(path: String): Unit = {
        // 删除目标路径
        try {
            FileTools.delPath(path)
        } catch {
            case ex: IOException => ex.printStackTrace()
        }
    }
}
