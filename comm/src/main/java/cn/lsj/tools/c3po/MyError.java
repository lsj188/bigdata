package cn.lsj.tools.c3po;

import java.sql.SQLException;

/**
 * Created by lsj on 2017/12/7.
 */
public class MyError extends RuntimeException {

    public MyError() {
        super();
    }

    public MyError(String msg, Exception e) {
        super(msg);
        e.printStackTrace();

    }

    public MyError(String msg, Exception e,String s) {
        super(msg);
        e.printStackTrace();

    }

}
