package me.savushkin.lab5;

import me.savushkin.common.LabHelper;
import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import oracle.jdbc.pool.OracleDataSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.logging.Logger;

public class Lab {
    private static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "";
    private static String ORACLE_PASS = "";
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
                    File jnfiDir = new File("JNDI");
                    if (!jnfiDir.exists())
                        jnfiDir.mkdir();
                    String file = "file:JNDI";
                    String dataSourceName = "myDatabase";
                    try {
                        //Create Hashtable to hold environment properties
                        //then open InitialContext
                        Hashtable environment = new Hashtable();
                        environment.put (Context.INITIAL_CONTEXT_FACTORY, sp);
                        environment.put (Context.PROVIDER_URL, file);
                        context = new InitialContext(environment);

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
                    OracleConnectionPoolDataSource ocpds = null;
                    PooledConnection pc_1 = null;
                    PooledConnection pc_2 = null;

                    try{
                        ocpds = new OracleConnectionPoolDataSource();
                        ocpds.setURL(ORACLE_URL);
                        ocpds.setUser(ORACLE_USER);
                        ocpds.setPassword(ORACLE_PASS);

                        //Create a pooled connection
                        pc_1 = ocpds.getPooledConnection();

                        //Open a Connection and a Statement object
                        Connection conn_1 = pc_1.getConnection();
                        Statement stmt = conn_1.createStatement();

                        //Build query string
                        String sql = "SELECT count(*) FROM v$session WHERE username = '" + ORACLE_USER.toUpperCase() + "'";

                        System.out.println(sql);

                        //Execute query and print results
                        ResultSet rs = stmt.executeQuery(sql);
                        rs.next();
                        String msg = "Total connections after ";
                        System.out.println(msg + "conn_1: " + rs.getString(1));

                        ///Open second logical connection and execute query
                        Connection conn_2 = pc_1.getConnection();
                        stmt = conn_2.createStatement();
                        rs = stmt.executeQuery(sql);
                        rs.next();
                        System.out.println(msg + "conn_2: " + rs.getString(1));

                        //Open second physical connection and execute query.
                        pc_2 = ocpds.getPooledConnection();
                        rs = stmt.executeQuery(sql);
                        rs.next();
                        System.out.println(msg + "pc_2: " + rs.getString(1));

                        //Close resources
                        conn_1.close();
                        conn_2.close();

                        //Standard error handling.
                    }catch(SQLException se){
                        //Handle errors for JDBC
                        se.printStackTrace();
                    }catch(Exception e){
                        //Handle errors for Class.forName
                        e.printStackTrace();
                    }finally{
                        //Finally clause used to close resources
                        try{
                            if(pc_1!=null)
                                pc_1.close();
                        }catch(SQLException se){
                            se.printStackTrace();
                        }finally{
                            try{
                                if(pc_2!=null)
                                    pc_2.close();
                            }catch(SQLException se){
                                se.printStackTrace();
                            }
                        }
                    }//end try

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
