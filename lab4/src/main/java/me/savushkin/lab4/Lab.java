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

    }

    //get property
    private Properties getProperties() {
        Properties prop = new Properties();
        InputStream input = null;
        try {

            input = new FileInputStream("database.properties");

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

        Properties prop = new Properties();
        OutputStream output = null;
        try {

            output = new FileOutputStream("database.properties");

            // set the properties value
            prop.setProperty("jdbc.drivers", "oracle.jdbc.OracleDriver");
            prop.setProperty("jdbcUrl", "jdbcoraclethin@localhost1521orbis");
            prop.setProperty("dbuser", "mkyong");
            prop.setProperty("dbpassword", "password");

            // save properties to project root folder
            prop.store(output, null);

        } catch (IOException io) {
            io.printStackTrace();
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
