package cn.lsj.jdbc

import java.sql.DriverManager

/**
  *
  */
object ThriftserverDemo {
    def main(args: Array[String]): Unit = {
        //add driver
        val driver = "org.apache.hive.jdbc.HiveDriver"
        Class.forName(driver)

        //get connection
        val (url, username, userpasswd) = ("jdbc:hive2://localhost:10000", "hive", "hive")
        val connection = DriverManager.getConnection(url, username, userpasswd)

        //get statement
        connection.prepareStatement("use lsj_test").execute()
        val sql = "select * from test1"
        val statement = connection.prepareStatement(sql)

        //get result
        val rs = statement.executeQuery()
        while (rs.next()) {
            println(s"${rs.getString(1)}:${rs.getString(2)}")
        }

        //close
        rs.close()
        statement.close()
        connection.close()
    }
}