package me.savushkin.lab4;

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

import static java.lang.System.getProperties;
import static java.lang.System.getProperty;

public class Lab {

    static Logger log = Logger.getLogger(Lab.class.getName());

    public static void main(String[] args) {

        Connection conn = null;
        //get properties
        Properties prop = getPropertiesFromFile();
        String driver = prop.getProperty("driver1");
        String jdbcUrl = prop.getProperty("jdbcUrl1");
        System.setProperty("driver", driver);
        System.setProperty("url", jdbcUrl);
        //setProperty();
        try {
            //Driver registration
            System.out.println("Driver: " + System.getProperty("driver"));
            Class.forName(System.getProperty("driver")).newInstance();


            //Create connection
            System.out.println("Connection to database...");
            System.out.println("Connection to " + getProperty("url"));
            conn = DriverManager.getConnection(getProperty("url"));
            //Executing the query
            Statement statement = conn.createStatement();
            String sql = "SELECT COUNT(*) FROM all_tables";
            statement.execute(sql);
            statement.close();
            conn.close();
        }catch (ClassNotFoundException e){
            System.out.println("Driver not found");
            e.printStackTrace();
    }catch (Exception e){
            e.printStackTrace();
        }

    }

    //get property
    private static Properties getPropertiesFromFile() {
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
