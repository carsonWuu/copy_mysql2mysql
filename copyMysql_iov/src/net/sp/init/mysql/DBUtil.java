package net.sp.init.mysql;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class DBUtil {
    private static String driver = "";
    private static String[] url ;
    private static String[] username ;
    private static String[] password ;
    private Connection conn = null;
    
    /*
     * 静态代码块
     */
    static{
        Properties pro = new Properties();
        try {
        	//这是将db.properties数据流放入内存中
            InputStream ins =  Thread.currentThread().getContextClassLoader().getResourceAsStream("db.properties");
            pro.load(ins);
            url=new String[2];
            username=new String[2];
            password=new String[2];
            
            driver=pro.getProperty("COMMON.driverClassName");
            url[0]=pro.getProperty("dbMaster.url");
            username[0]=pro.getProperty("dbMaster.username");
            password[0]=pro.getProperty("dbMaster.password");
            
            
            url[1]=pro.getProperty("dbSlave.url");
            username[1]=pro.getProperty("dbSlave.username");
            password[1]=pro.getProperty("dbSlave.password");
            ins.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public Connection getMasterConnection() {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url[0], username[0], password[0]);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    
    public Connection getSlaveConnection() {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url[1], username[1], password[1]);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    
    /**
     * 关闭链接
     * @param con
     * @param ps
     * @param rs
     */
    public static void close(Connection conn,PreparedStatement ps,ResultSet rs){
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(ps!=null){
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}