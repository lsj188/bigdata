package cn.lsj.tools.c3po;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.*;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class C3p0Utils {
    private static final Object key = new Object();
    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(C3p0Utils.class.getName());
    //连接池集合
    private static Map<String, ComboPooledDataSource> dataSources = new ConcurrentHashMap<String, ComboPooledDataSource>();

    /**
     * 从连接池中取用一个连接
     *
     * @param connName
     * @return
     */
    public static Connection getConnection(String connName) {
        synchronized (key) {
            try {
                ComboPooledDataSource dataSource = dataSources.get(connName);
                if (dataSource == null) {
                    dataSource = new ComboPooledDataSource(connName);
                    dataSources.put(connName, new ComboPooledDataSource(connName));
                }
                return dataSource.getConnection();
            } catch (Exception e) {
                logger.error("Exception in C3p0Utils!", e);
                throw new MyError("数据库连接出错!", e);
            }
        }
    }

    /**
     * 获取JDBC连接
     *
     * @param jdbc_class_forname
     * @param jdbc_url
     * @param jdbc_user
     * @param jdbc_password
     * @return
     */
    public static Connection getJDBCConnection(String jdbc_class_forname, String jdbc_url, String jdbc_user, String jdbc_password) {
        Connection con = null;
        try {
            Class.forName(jdbc_class_forname).newInstance();
            con = DriverManager.getConnection(jdbc_url, jdbc_user, jdbc_password);
            System.out.println("connection [jdbc] sucess......");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }

    /**
     * 获取connection从配置文件app.properties中获取
     *
     * @return
     */
    public static Connection getJDBCConnection() {
        //app properties
        Hashtable configureHashInfo = AppPropertiesConfigureInfo.getInstance().getConfigureHashInfo();

        //驱动
        String jdbc_class_forname = (String) configureHashInfo.get("jdbc_class_forname");
        //url
        String jdbc_url = (String) configureHashInfo.get("jdbc_url");
        //user
        String jdbc_user = (String) configureHashInfo.get("jdbc_user");
        //password
        String jdbc_password = (String) configureHashInfo.get("jdbc_password");

        Connection con = getJDBCConnection(jdbc_class_forname, jdbc_url, jdbc_user, jdbc_password);
        return con;
    }

    /**
     * 释放连接回连接池
     *
     * @param conn
     * @param pst
     * @param rs
     */
    public static void close(Connection conn, PreparedStatement pst, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("Exception in C3p0Utils!", e);
                throw new MyError("数据库连接关闭出错!", e);
            }
        }
        if (pst != null) {
            try {
                pst.close();
            } catch (SQLException e) {
                logger.error("Exception in C3p0Utils!", e);
                throw new MyError("数据库连接关闭出错!", e);
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Exception in C3p0Utils!", e);
                throw new MyError("数据库连接关闭出错!", e);
            }
        }
    }
}