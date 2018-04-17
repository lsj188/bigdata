package cn.lsj.tools.c3po;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by lsj on 2017/12/7.
 */
public class DBUtil_BO {
    public Connection conn = null;
    public PreparedStatement st = null;
    public ResultSet rs = null;

    public DBUtil_BO() {
        super();
    }
}