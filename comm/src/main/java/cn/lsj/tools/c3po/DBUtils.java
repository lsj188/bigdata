package cn.lsj.tools.c3po;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.UUID;

/**
 * Created by lsj on 2017/12/7.
 */
public class DBUtils {
    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DBUtils.class.getName());
    public static final Integer PAGE_SIZE=10;

    /**
     * 关闭连接
     *
     * @param _conn
     * @param _st
     * @param _rs
     */
    private static void realseSource(Connection _conn, PreparedStatement _st, ResultSet _rs) {
        C3p0Utils.close(_conn, _st, _rs);
    }

    /**
     * 关闭连接
     *
     * @param _vo
     */
    public static void realseSource(DBUtil_BO _vo) {
        if (_vo != null) {
            realseSource(_vo.conn, _vo.st, _vo.rs);
        }
    }

    /**
     * 注意：查询操作完成后，因为还需提取结果集中信息，所以仍保持连接，
     * 在结果集使用完后才通过DBUtils.realseSource()手动释放连接
     *
     * @param vo
     */
    public static void executeQuery(DBUtil_BO vo) {
        try {

            vo.rs = vo.st.executeQuery();

        } catch (SQLException e) {
            realseSource(vo);
            String uuid = UUID.randomUUID().toString().replaceAll("-", "");
            logger.error("UUID:" + uuid + ", SQL语法有误: ", e);
            throw new MyError("err.user.dao.jdbc", e, uuid);

        }
    }

    /**
     * 而update操作完成后就可以直接释放连接了，所以在方法末尾直接调用了realseSourse()
     *
     * @param vo
     */
    public static void executeUpdate(DBUtil_BO vo) {

        Connection conn = vo.conn;
        PreparedStatement st = vo.st;
        //保存当前自动提交模式
        boolean autoCommit = false;
        try {
            autoCommit = conn.getAutoCommit();
            vo.conn.setAutoCommit(false);
            st.executeUpdate();
            vo.conn.commit();
            vo.conn.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            try {
                vo.conn.rollback();
                vo.conn.setAutoCommit(autoCommit);
                realseSource(conn, st, null);
                String uuid = UUID.randomUUID().toString().replaceAll("-", "");
                logger.error("UUID:" + uuid + ", SQL语法有误: ", e);
                throw new MyError("err.user.dao.jdbc", e, uuid);
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
        realseSource(conn, st, null);

    }

    /**
     * 批量执行
     *
     * @param vo
     */
    public static void executeBatch(DBUtil_BO vo) {

        Connection conn = vo.conn;
        PreparedStatement st = vo.st;
        //保存当前自动提交模式
        boolean autoCommit = false;
        try {
            autoCommit = conn.getAutoCommit();
            vo.conn.setAutoCommit(false);
            st.executeBatch();
            vo.conn.commit();
            vo.conn.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            try {
                vo.conn.rollback();
                vo.conn.setAutoCommit(autoCommit);
                realseSource(conn, st, null);
                String uuid = UUID.randomUUID().toString().replaceAll("-", "");
                logger.error("UUID:" + uuid + ", SQL语法有误: ", e);
                throw new MyError("err.user.dao.jdbc", e, uuid);
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
        realseSource(conn, st, null);

    }

    /**
     * 将resultSet转化为JSONObject
     *
     * @param rs
     * @return
     */
    public static JSONObject resultSetToJsonObject(ResultSet rs) {
        // json对象
        JSONObject jsonObj = new JSONObject();
        // 获取列数
        ResultSetMetaData metaData = null;
        try {
            metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            // 遍历ResultSet中的每条数据
            if (rs.next()) {
                // 遍历每一列
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    String value = rs.getString(columnName);
                    jsonObj.put(columnName, value);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return jsonObj;
    }

    /**
     * 将resultSet转化为JSON数组
     *
     * @param rs
     * @return
     */
    public static JSONArray resultSetToJsonArry(ResultSet rs) {
        // json数组
        JSONArray array = new JSONArray();

        // 获取列数
        ResultSetMetaData metaData = null;
        try {
            metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 遍历ResultSet中的每条数据
            while (rs.next()) {
                JSONObject jsonObj = new JSONObject();

                // 遍历每一列
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    String value = rs.getString(columnName);
                    jsonObj.put(columnName, value);
                }
                array.put(jsonObj);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return array;
    }

}

