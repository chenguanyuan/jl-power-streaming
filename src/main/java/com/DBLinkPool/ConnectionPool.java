package com.DBLinkPool;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 创建JDBC连接池
 */
public class ConnectionPool {
    private static ComboPooledDataSource dataSource = new ComboPooledDataSource();
    static {

        Properties properties = new Properties();
        InputStream in = ConnectionPool.class.getClassLoader().getResourceAsStream("DB.properties");
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String dbName = properties.getProperty("dbname");

        try {
            //mysql: com.mysql.jdbc.Driver
            //oracle: oracle.jdbc.driver.OracleDriver
            String driverClass = null;
            if("oracle".equals(dbName)){driverClass = "oracle.jdbc.driver.OracleDriver";}
            else{driverClass="com.mysql.jdbc.Driver";}
            dataSource.setDriverClass(driverClass);
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }

        //mysql: jdbc:mysql://master:3306/jltest
        //oracle: jdbc:oracle:thin:@//192.168.2.130:1521/vms

        String jdbcUrl = null;
        if("oracle".equals(dbName)){jdbcUrl = "jdbc:oracle:thin:@//192.168.2.130:1521/vms";}
        else{jdbcUrl="jdbc:mysql://master:3306/jltest";}
        dataSource.setJdbcUrl(jdbcUrl);//设置连接数据库的URL

        //oracle: vmsadmin

        String userName = null;
        if("oracle".equals(dbName)){userName = "vmsadmin";}
        else{userName="root";}
        dataSource.setUser(userName);//设置连接数据库的用户名
        //oracle: VmsOra#15
        String password = null;
        if("oracle".equals(dbName)){password = "VmsOra#15";}
        else{password="root";}
        dataSource.setPassword(password);//设置连接数据库的密码

        dataSource.setMaxPoolSize(40);//设置连接池的最大连接数

        dataSource.setMinPoolSize(2);//设置连接池的最小连接数

        dataSource.setInitialPoolSize(10);//设置连接池的初始连接数

        dataSource.setMaxStatements(100);//设置连接池的缓存Statement的最大数
    }

    public static Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void returnConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
