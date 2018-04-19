package cn.lsj.tools.c3po;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import cn.lsj.tools.c3po.*;
/**
 * Created by lsj on 2017/12/7.
 */

public class TestConJDBC {
    private static Map<String, Integer> dataSources = new ConcurrentHashMap<String, Integer>();

    public static void main(String[] args) throws Exception {
        TestConJDBC tesconn = new TestConJDBC();
//        tesconn.jdbcTest();
//        tesconn.c3p0ConnQuery();
//        tesconn.c3p0ConnBatch();
        tesconn.c3p0HiveConnQuery();
//        System.out.println(tesconn.dataSources.get("mysql"));
//        tesconn.dataSources.put("mysql", 1);
//        System.out.println(tesconn.dataSources.get("mysql"));



    }

    public void c3p0ConnBatch() {
        DBUtil_BO dbBo = new DBUtil_BO();
        String sql=null;
        try {
            dbBo.conn = C3p0Utils.getConnection("mysql");//取用一个连接
            dbBo.st = dbBo.conn.prepareStatement("use test"); //选择数据库
            dbBo.st.execute();
            dbBo.st.clearBatch();
            for (int i=0;i<100;i++)
            {
               dbBo.st.addBatch("insert into student(NO,name) values('2012001"+i+"','陶伟基"+i+"')");
            }
            DBUtils.executeBatch(dbBo);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void c3p0HiveConnQuery() {
        String sql = "select * from lsj_test.test1";
        DBUtil_BO dbBo = new DBUtil_BO();
        try {
            dbBo.conn = C3p0Utils.getConnection("hive");//取用一个连接
            dbBo.st = dbBo.conn.prepareStatement(sql);
            dbBo.rs = dbBo.st.executeQuery();
            while (dbBo.rs.next()) {
                String id = dbBo.rs.getString(1);
                String name = dbBo.rs.getString(2);
                System.out.println("id: "+id+"\tname: "+name);
            }

        } catch (SQLException e1) {
            e1.printStackTrace();
            //结果集遍历完了，手动释放连接回连接池
            DBUtils.realseSource(dbBo);
        }
        //结果集遍历完了，手动释放连接回连接池
        DBUtils.realseSource(dbBo);
    }

    public void c3p0ConnQuery() {
        String sql = "select user,host from mysql.user where user=?";
        DBUtil_BO dbBo = new DBUtil_BO();
        try {
            dbBo.conn = C3p0Utils.getConnection("mysql");//取用一个连接
            dbBo.st = dbBo.conn.prepareStatement(sql);
            dbBo.st.setString(1, "root");
            dbBo.rs = dbBo.st.executeQuery();
            while (dbBo.rs.next()) {
                String user = dbBo.rs.getString("user");
                String host = dbBo.rs.getString("host");
                System.out.println("==============user:" + user + ",host:" + host);
            }

        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        //结果集遍历完了，手动释放连接回连接池
        DBUtils.realseSource(dbBo);
    }

    public void jdbcTest() throws SQLException {
        Connection conn = null;
        String sql;
        // MySQL的JDBC URL编写方式：jdbc:mysql://主机名称：连接端口/数据库的名称?参数=值
        // 避免中文乱码要指定useUnicode和characterEncoding
        // 执行数据库操作之前要在数据库管理系统上创建一个数据库，名字自己定，
        // 下面语句之前就要先创建javademo数据库
        String url = "jdbc:mysql://localhost:3306/test?"
                + "user=root&password=lsj123&useUnicode=true&characterEncoding=UTF8";

        try {
            // 之所以要使用下面这条语句，是因为要使用MySQL的驱动，所以我们要把它驱动起来，
            // 可以通过Class.forName把它加载进去，也可以通过初始化来驱动起来，下面三种形式都可以
            Class.forName("com.mysql.jdbc.Driver");// 动态加载mysql驱动
            // or:
            // com.mysql.jdbc.Driver driver = new com.mysql.jdbc.Driver();
            // or：
            // new com.mysql.jdbc.Driver();

            System.out.println("成功加载MySQL驱动程序");
            // 一个Connection代表一个数据库连接
            conn = DriverManager.getConnection(url);
            // Statement里面带有很多方法，比如executeUpdate可以实现插入，更新和删除等
            Statement stmt = conn.createStatement();
            sql = "drop table if exists student ;";
            int result1 = stmt.executeUpdate(sql);// executeUpdate语句会返回一个受影响的行数，如果返回-1就没有成功
            sql = "create table student(NO char(20),name varchar(20),primary key(NO))";
            int result = stmt.executeUpdate(sql);// executeUpdate语句会返回一个受影响的行数，如果返回-1就没有成功
            if (result != -1) {
                System.out.println("创建数据表成功");
                sql = "insert into student(NO,name) values('2012001','陶伟基')";
                result = stmt.executeUpdate(sql);
                sql = "insert into student(NO,name) values('2012002','周小俊')";
                result = stmt.executeUpdate(sql);
                sql = "select * from student";
                ResultSet rs = stmt.executeQuery(sql);// executeQuery会返回结果的集合，否则返回空值
                System.out.println("学号\t姓名");
                while (rs.next()) {
                    System.out
                            .println(rs.getString(1) + "\t" + rs.getString(2));// 入如果返回的是int类型可以用getInt()
                }
            }
        } catch (SQLException e) {
            System.out.println("MySQL操作错误");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.close();
        }
    }

}
