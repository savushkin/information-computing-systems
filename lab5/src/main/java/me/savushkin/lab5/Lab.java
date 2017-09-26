package me.savushkin.lab5;

import me.savushkin.common.LabHelper;
import oracle.jdbc.pool.OracleDataSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.logging.Logger;

public class Lab {
    private static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "s182190";
    private static String ORACLE_PASS = "tny395";
    private static String POSTGRES_URL = "jdbc:postgresql://pg:5432/ucheb";
    private static String POSTGRES_DRIVER = "org.postgresql.Driver";
    private static String POSTGRES_USER = "";
    private static String POSTGRES_PASS = "";

    static Logger log = Logger.getLogger(Lab.class.getName());

    public static void main(String[] args) {
        if (args.length > 0) {
            switch (args[0]) {
                case "task1": {
                    try {
                        OracleDataSource ods = new OracleDataSource();

                        ods.setUser(ORACLE_USER);
                        ods.setPassword(ORACLE_PASS);
                        ods.setDriverType("thin");
                        ods.setDatabaseName("orbis");
                        ods.setServerName("localhost");
                        ods.setPortNumber(1521);

                        Connection connection = ods.getConnection();
                        if (connection != null) {
                            System.out.println("Connection successful!!!");
                            connection.close();
                        } else {
                            System.out.println("Connection fail!!!");
                        }

                    } catch (SQLException se) {
                        se.printStackTrace();
                    }
                    System.out.println("Goodbye!");
                    break;
                }
                case "task2": {
                    Connection connection = null;
                    Statement statement = null;
                    Context context = null;
                    DataSource dataSource = null;
                    String sp = "com.sun.jndi.fscontext.RefFSContextFactory";
                    String file = "file:JNDI";
                    String dataSourceName = "myDatabase";
                    try {
                        //Create Hashtable to hold environment properties
                        //then open InitialContext
                        Hashtable env = new Hashtable();
                        env.put (Context.INITIAL_CONTEXT_FACTORY, sp);
                        env.put (Context.PROVIDER_URL, file);
                        context = new InitialContext(env);

                        //Bind the DataSource object
                        bindDataSource(context, dataSourceName);

                        //Retrieve the DataSource object
                        dataSource = (DataSource) context.lookup(dataSourceName);

                        //Open a connection, submit query, and print results
                        connection = dataSource.getConnection();
                        statement = connection.createStatement();

                        LabHelper.printResultSet(statement.executeQuery("SELECT count(*) FROM all_tables"));

                        // Close the connections to the data store resources
                        statement.close();
                    } catch (NamingException | SQLException e){
                        e.printStackTrace();
                    } finally {
                        try{
                            if (context != null)
                                context.close();
                        }catch (NamingException ne){
                            ne.printStackTrace();
                        }finally{
                            try{
                                if (connection != null)
                                    connection.close();
                            } catch (SQLException se){
                                se.printStackTrace();
                            }
                        }
                    }
                    System.out.println("Goodbye!");
                    break;
                }
                case "task3": {
                    System.out.println("Goodbye!");
                    break;
                }
                default:
                    System.err.println("Undefined argument");
                    break;
            }
        }
    }

    // task 2
    public static void bindDataSource(Context context, String dataSourceName)
            throws SQLException, NamingException{

        //Create an OracleDataSource instance
        OracleDataSource oracleDataSource = new OracleDataSource();

        //Set the connection parameters
        oracleDataSource.setUser(ORACLE_USER);
        oracleDataSource.setPassword(ORACLE_PASS);
        oracleDataSource.setDriverType("thin");
        oracleDataSource.setDatabaseName("orbis");
        oracleDataSource.setServerName("localhost");
        oracleDataSource.setPortNumber(1521);

        //Bind the DataSource
        context.rebind (dataSourceName, oracleDataSource);
    }
}
