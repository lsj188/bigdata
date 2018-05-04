package cn.lsj.demo

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._



case class Crouses(type1: String, level: String, title: String, url: String, image_path: String, image_url: String, student: Int, introduction: String)
{
    override def toString: String ={
       return type1+'|'+level+'|'+title+'|'+url+'|'+image_path+'|'+image_url+'|'+student+'|'+introduction
    }
}
class TableAndSql {

    def csvTabSource(args: Map[String,String]): Unit = {
        val inPath="e:/git/testData/test_csv.csv"
        val outPath="e:/git/testData/flink_out.csv"
        val hdfsOutPath="hdfs://localhost:9000/tmp/test/flink/crouses1"
        val hdfsInPath="hdfs://localhost:9000/tmp/test/flink/crouses"

        val schemaArray = Array("type1", "level", "title", "url", "image_path", "image_url", "student", "introduction")
        //        val csvTableSource1 = bEnv.readCsvFile[Crouses]("e:/git/testData/test_csv.csv","\n", ",")
        // get execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment
        val tEnv = TableEnvironment.getTableEnvironment(env)
        val csvTableSource: DataSet[Crouses] = env.readCsvFile[Crouses](
            hdfsInPath,
            fieldDelimiter = "|",
            includedFields = Array(0, 1, 2, 3, 4, 5, 6, 7))

//        csvTableSource.map(_.toString).writeAsText(hdfsOutPath,WriteMode.OVERWRITE)
        //DataSet2Table
        val lineTable=csvTableSource.toTable(tEnv, 'type1,'level,'title,'url,'image_path,'image_url,'student,'introduction)
        val resultT=lineTable.groupBy('type1,'level).select('type1,'level,'student.sum as 'student,'level.count as 'cnt).orderBy('student.desc)

        //注册成表
        val tmpTable=tEnv.registerTable("test_tmp",lineTable)
        val tmpResult=tEnv.sqlQuery("select * from test_tmp where student>2000")
        tmpResult.toDataSet[(String,String,Integer,Long)].print()


        //写HDFS
        resultT.toDataSet[(String,String,Integer,Long)].writeAsText(hdfsOutPath,WriteMode.OVERWRITE)
//        resultT.toDataSet[(String,String,Integer,Long)].writeAsCsv(outPath,"/n","|",WriteMode.OVERWRITE)

        env.execute("Flink scala (Table API Expression) Example")
    }
}
