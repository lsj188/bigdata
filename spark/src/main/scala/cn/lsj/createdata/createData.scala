package cn.lsj.createdata

import cn.lsj.comm.SparkInit
import cn.lsj.tools.DateTools
import org.apache.spark.sql.{Row, SaveMode}
import cn.lsj.comm.SparkInit._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by lsj on 2017/9/20.
  */
object createData {

    def main(args: Array[String]) {
//        table2txt()
    table2parquet()
    }


    def table2txt(): Unit = {
        val outPutFile = "E:\\git\\testData\\test.courses"
        val (sc, sqlContext) = initSpark("local", "createData")
        val startTime = "2017-01-01 00:00:00"

        //读取mysql表数据
        val tabDF = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test")
            .option("dbtable", "courses")
            .option("user", "root")
            .option("password", "lsj123")
            .load()
        tabDF.cache()
        tabDF.show(10)
        delFilePath(outPutFile)
        // 将DataFrame转为Rdd
        for (i <- 0 to 2) {
            val typeNum = i % 10
            val typeNum1 = i % 50
            val tabRDD = tabDF.rdd.flatMap((row: Row) => for (col1 <- row.getString(0).split(","))
                yield List(col1 + typeNum1,
                    row.getString(1) + typeNum,
                    row.getString(2) + i,
                    row.getString(3) + i,
                    row.getString(4) + i,
                    row.getString(5) + i,
                    row.getString(6),
                    row.getString(7),
                    DateTools.dayCalculate(startTime, i).substring(0, 10)
                    //        row.getTimestamp(8)
                ).mkString(","))

            tabRDD.saveAsTextFile(outPutFile + "\\in_time=" + DateTools.dayCalculate(startTime, i).substring(0, 10))
        }

        sc.stop()
    }

    def table2parquet(): Unit = {
        val outPutFile = "hdfs://localhost:9000/user/hive/warehouse/lsj_test.db/t_parquet"
        val (sc, sqlContext) = initSpark("local", "createData")
        val startTime = "2017-01-01 00:00:00"

        //读取mysql表数据
        val tabDF = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/test")
            .option("dbtable", "courses")
            .option("user", "root")
            .option("password", "lsj123")
            .load()
        tabDF.cache()
        tabDF.show(10)
//        delFilePath(outPutFile)
        // 将DataFrame转为Rdd
        for (i <- 0 to 2) {
            val typeNum = i % 10
            val typeNum1 = i % 50
            val tabRDD = tabDF.rdd.flatMap((row: Row) => for (col1 <- row.getString(0).split(","))
                yield List(col1 + typeNum1,
                    row.getString(1) + typeNum,
                    row.getString(2) + i,
                    row.getString(3) + i,
                    row.getString(4) + i,
                    row.getString(5) + i,
                    row.getString(6),
                    row.getString(7)
                    //,DateTools.dayCalculate(startTime,i).substring(0,10)
                    //        row.getTimestamp(8)
                ).mkString(","))


            //运行时从别处获取schema
            val schemaArray = Array("type","level","title","url","image_path","image_url","student","introduction")

            //创建schema
            val schema = StructType(schemaArray.map(col => StructField(col, StringType, true)))

            //将文本转为Rdd
            val rowRdd = tabRDD.map(_.split(",")).map(cols => Row(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7)))

            //将schema应用于RDD
            val parquetDF = sqlContext.createDataFrame(rowRdd, schema)
            //RDD转DF
            parquetDF.write.mode(SaveMode.Overwrite).parquet(outPutFile + "/in_time=" + DateTools.dayCalculate(startTime, i).substring(0, 10))
        }

        sc.stop()
    }


}
