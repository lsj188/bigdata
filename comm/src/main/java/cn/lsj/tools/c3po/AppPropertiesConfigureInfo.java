package cn.lsj.tools.c3po;

import java.util.Hashtable;
import java.util.ResourceBundle;

/**
 *
 */
public class AppPropertiesConfigureInfo
{
    private static final Object key = new Object();
    private static AppPropertiesConfigureInfo appPropertiesConfigureInfo;
    private static Hashtable configureHashInfo;

    /**
     *
     * @return
     */
    public Hashtable getConfigureHashInfo ()
    {
        return configureHashInfo;
    }

    private AppPropertiesConfigureInfo()
    {
        configureHashInfo = new Hashtable();
        ResourceBundle rb = ResourceBundle.getBundle("app");
        //rdbms jdbc
        // 驱动
        String jdbc_class_forname = rb.getString("jdbc_class_forname");
        configureHashInfo.put("jdbc_class_forname",jdbc_class_forname);
        //url
        String jdbc_url =rb.getString("jdbc_url");
        configureHashInfo.put("jdbc_url",jdbc_url);
        //用户名
        String jdbc_user = rb.getString("jdbc_user");
        configureHashInfo.put("jdbc_user",jdbc_user);
        //密码
        String jdbc_password = rb.getString("jdbc_password");
        configureHashInfo.put("jdbc_password",jdbc_password);

        //hive jdbc
        // 驱动
        String hive_jdbc_class_forname = rb.getString("hive_jdbc_class_forname");
        configureHashInfo.put("hive_jdbc_class_forname",hive_jdbc_class_forname);
        //url
        String hive_jdbc_url = rb.getString("hive_jdbc_url");
        configureHashInfo.put("hive_jdbc_url",hive_jdbc_url);
        //user
        String hive_jdbc_user = rb.getString("hive_jdbc_user");
        configureHashInfo.put("hive_jdbc_user",hive_jdbc_user);
        //password
        String hive_jdbc_password = rb.getString("hive_jdbc_password");
        configureHashInfo.put("hive_jdbc_password",hive_jdbc_password);
    }
    public static AppPropertiesConfigureInfo getInstance()
    {
        if (appPropertiesConfigureInfo == null)
        {
            synchronized (key)
            {
                if (appPropertiesConfigureInfo ==null)
                {
                    appPropertiesConfigureInfo =new AppPropertiesConfigureInfo();
                }
            }
        }
        return appPropertiesConfigureInfo;
    }
    public static void main(String[] args)
    {
        AppPropertiesConfigureInfo appPropertiesConfigureInfo =AppPropertiesConfigureInfo.getInstance();
        Hashtable hashtable = appPropertiesConfigureInfo.getConfigureHashInfo();
        String jdbc_class_forname = (String)hashtable.get("jdbc_class_forname");
        System.out.println("jdbc_class_forname=================="+jdbc_class_forname);
        String jdbc_url = (String)hashtable.get("jdbc_url");
        System.out.println("jdbc_url==============="+jdbc_url);
        String jdbc_user = (String)hashtable.get("jdbc_user");
        System.out.println("jdbc_user=============="+jdbc_user);
        String jdbc_password = (String)hashtable.get("jdbc_password");
        System.out.println("jdbc_password=============="+jdbc_password);
    }
}
