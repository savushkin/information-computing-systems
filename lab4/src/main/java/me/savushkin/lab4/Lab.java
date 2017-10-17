package me.savushkin.lab4;

import me.savushkin.common.LabHelper;
import oracle.jdbc.pool.OracleDataSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Properties;
import java.util.logging.Logger;

public class Lab {

    static Logger log = Logger.getLogger(Lab.class.getName());

    public static void main(String[] args) {

        Connection conn=null;
        //get properties
        Properties prop = getProperties();
        String driver = prop.getProperty("jdbc.drivers");
        String jdbcUrl=prop.getProperty("jdbcUrl");
        System.setProperty("drivers",driver);
        System.setProperty("url",jdbcUrl);
        //setProperty();
        try
        {
        //Driver registration
            System.out.println("Driver " + System.getProperty("drivers"));
            //Class.forName(System.getProperty("drivers"));

        //Create connection
            System.out.println("Connection to database...");
            System.out.println("Connection to "+ System.getProperty("url"));
            //conn=DriverManager.getConnection(System.getProperty("url"));
        //Executing the query
            //Statement statement=conn.createStatement();
            String sql="SELECT COUNT(*) FROM all_tables";
            //statement.execute(sql);
            //statement.close();
            //conn.close();
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    //get property
    private static Properties getProperties() {
        Properties prop = new Properties();
        InputStream input = null;
        try {

            input = Lab.class.getResourceAsStream("/database.properties");
            // load a properties file
            prop.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }

    //set property
    private static void setProperty() {

    }
}
