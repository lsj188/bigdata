package cn.lsj.demo

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._




class TableAndSql {
    case class Crouses(type1: String, level: String, title: String, url: String, image_path: String, image_url: String, student: Int, introduction: String)

    def csvTabSource(args: Map[String,String]): Unit = {
        val inPath="e:/git/testData/test_csv.csv"
        val outPath="e:/git/testData/flink_out.csv"
        val schemaArray = Array("type1", "level", "title", "url", "image_path", "image_url", "student", "introduction")
        //        val csvTableSource1 = bEnv.readCsvFile[Crouses]("e:/git/testData/test_csv.csv","\n", ",")
        // get execution environment
        val env = ExecutionEnvironment.getExecutionEnvironment
        val tEnv = TableEnvironment.getTableEnvironment(env)
        val csvTableSource: DataSet[Crouses] = env.readCsvFile[Crouses](
            "e:/git/testData/test_csv.csv",
            fieldDelimiter = ",",
            includedFields = Array(0, 1, 2, 3, 4, 5, 6, 7))

        //DataSet2Table
        val lineTable=csvTableSource.toTable(tEnv, 'type1,'level,'title,'url,'image_path,'image_url,'student,'introduction)
        val resultT=lineTable.groupBy('type1,'level).select('type1,'level,'student.sum as 'student,'level.count as 'cnt).orderBy('student.desc)

        resultT.writeAsCsv(outPath,"\n",",",WriteMode.OVERWRITE)

        env.execute("Flink scala (Table API Expression) Example")
    }
}
