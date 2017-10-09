package me.savushkin.lab6;

import me.savushkin.common.LabHelper;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.rowset.OracleCachedRowSet;
import oracle.jdbc.rowset.OracleJDBCRowSet;
import oracle.sql.ROWID;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.Date;
import java.sql.RowId;
import java.sql.SQLException;
import java.util.Hashtable;

public class Lab {
    private final static String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:orbis";
    private final static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private final static String ORACLE_USER = "";
    private final static String ORACLE_PASS = "";
    private final static String POSTGRES_URL = "jdbc:postgresql://pg:5432/ucheb";
    private final static String POSTGRES_DRIVER = "org.postgresql.Driver";
    private final static String POSTGRES_USER = "";
    private final static String POSTGRES_PASS = "";

    private final static String DATA_SOURCE_NAME = "myDatabase";

    private final static String CRS_FILE_LOC_TASK3 ="cachedrs_task3.crs";
    private final static String CRS_FILE_LOC_TASK4 ="cachedrs_task4.crs";

    private static Context context = null;

    public static void main(String[] args) {
        if (args.length > 0) {
            switch (args[0]) {
                case "task1": {
                    try {
                        OracleJDBCRowSet oracleJDBCRowSet = new OracleJDBCRowSet();
                        oracleJDBCRowSet.setUrl(ORACLE_URL);
                        oracleJDBCRowSet.setUsername(ORACLE_USER);
                        oracleJDBCRowSet.setPassword(ORACLE_PASS);
                        oracleJDBCRowSet.setCommand("SELECT count(*) FROM н_люди");
                        oracleJDBCRowSet.execute();
                        LabHelper.printResultSet(oracleJDBCRowSet);
                    } catch (SQLException se) {
                        se.printStackTrace();
                    }
                    break;
                }
                case "task2": {
                    try {
                        OracleJDBCRowSet oracleJDBCRowSet = new OracleJDBCRowSet();
                        oracleJDBCRowSet.setUrl(ORACLE_URL);
                        oracleJDBCRowSet.setUsername(ORACLE_USER);
                        oracleJDBCRowSet.setPassword(ORACLE_PASS);
                        oracleJDBCRowSet.setCommand("SELECT * FROM н_люди WHERE ид = ?");
                        oracleJDBCRowSet.setInt(1,161148);
                        oracleJDBCRowSet.execute();
                        LabHelper.printResultSet(oracleJDBCRowSet);
                    } catch (SQLException se) {
                        se.printStackTrace();
                    }
                    break;
                }
                case "task3": {
                    File jnfiDir = new File("JNDI");
                    if (!jnfiDir.exists())
                        jnfiDir.mkdir();

                    try {
                        Hashtable environment = new Hashtable();
                        environment.put (Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
                        environment.put (Context.PROVIDER_URL, "file:JNDI");
                        context = new InitialContext(environment);

                        bindDataSource(context, DATA_SOURCE_NAME);

                        //Create serialized CachedRowSet
                        writeCachedRowSet();

                        //Create CachedRowSet from serialized object
                        OracleCachedRowSet oracleCachedRowSet = readCachedRowSet();

                        //Display values
                        LabHelper.printResultSet(oracleCachedRowSet);
                        //Close resource
                        oracleCachedRowSet.close();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    break;
                }
                case "task4": {
                    try {
                        System.out.println("load data...");
                        OracleCachedRowSet oracleCachedRowSet = new OracleCachedRowSet();
                        oracleCachedRowSet.setUrl(ORACLE_URL);
                        oracleCachedRowSet.setUsername(ORACLE_USER);
                        oracleCachedRowSet.setPassword(ORACLE_PASS);
                        oracleCachedRowSet.setCommand("SELECT * FROM номера_телефонов");
                        oracleCachedRowSet.execute();
                        LabHelper.printResultSet(oracleCachedRowSet);
                        FileOutputStream fileOutputStream = new FileOutputStream(CRS_FILE_LOC_TASK4);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
                        objectOutputStream.writeObject(oracleCachedRowSet);
                        objectOutputStream.close();

                        System.out.println("read data...");
                        FileInputStream fileInputStream = new FileInputStream(CRS_FILE_LOC_TASK4);
                        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                        oracleCachedRowSet = (OracleCachedRowSet)objectInputStream.readObject();
                        fileInputStream.close();
                        objectInputStream.close();
                        oracleCachedRowSet.setCommand("SELECT * FROM номера_телефонов");
                        oracleCachedRowSet.execute();
                        LabHelper.printResultSet(oracleCachedRowSet);

                        System.out.println("update row...");
                        oracleCachedRowSet.setReadOnly(false);
                        oracleCachedRowSet.first();
                        oracleCachedRowSet.updateString(2,"1131313");
                        oracleCachedRowSet.updateRow();
                        oracleCachedRowSet.acceptChanges();
                        LabHelper.printResultSet(oracleCachedRowSet);

                        System.out.println("update row cancel...");
                        oracleCachedRowSet.setReadOnly(false);
                        oracleCachedRowSet.last();
                        oracleCachedRowSet.updateString(2,"42424242");
                        oracleCachedRowSet.updateRow();
                        oracleCachedRowSet.cancelRowUpdates();
                        LabHelper.printResultSet(oracleCachedRowSet);

                        System.out.println("insert row...");
                        oracleCachedRowSet.moveToInsertRow();
                        oracleCachedRowSet.updateInt(1,120848);
                        oracleCachedRowSet.updateString(2,"9332323");
                        oracleCachedRowSet.updateDate(3, new Date(System.currentTimeMillis()));
                        oracleCachedRowSet.updateInt(4,1);
                        oracleCachedRowSet.insertRow();
                        oracleCachedRowSet.acceptChanges();
                        LabHelper.printResultSet(oracleCachedRowSet);

                        System.out.println("insert row cancel...");
                        oracleCachedRowSet.moveToInsertRow();
                        oracleCachedRowSet.updateInt(1,120848);
                        oracleCachedRowSet.updateString(2,"9332323");
                        oracleCachedRowSet.updateDate(3, new Date(System.currentTimeMillis()));
                        oracleCachedRowSet.updateInt(4,1);
                        oracleCachedRowSet.insertRow();
                        oracleCachedRowSet.restoreOriginal();
                        LabHelper.printResultSet(oracleCachedRowSet);
                        oracleCachedRowSet.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    break;
                }
                default:
                    System.err.println("Undefined argument");
                    break;
            }
        }
        System.out.println("Goodbye!");
    }

    public static void writeCachedRowSet() throws Exception {
        Connection connection;
        DataSource dataSource;
        dataSource = (DataSource) context.lookup(DATA_SOURCE_NAME);
        connection = dataSource.getConnection();

        //Instantiate a CachedRowSet object, set connection parameters
        OracleCachedRowSet oracleCachedRowSet = new OracleCachedRowSet();

        //Set and execute the command. Notice the parameter query.
        oracleCachedRowSet.setCommand("SELECT * FROM н_люди WHERE ид = ?");
        oracleCachedRowSet.setInt(1,161148);
        oracleCachedRowSet.execute(connection);

        //Serialize CachedRowSet object.
        FileOutputStream fileOutputStream = new FileOutputStream(CRS_FILE_LOC_TASK3);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        objectOutputStream.writeObject(oracleCachedRowSet);
        objectOutputStream.close();
        oracleCachedRowSet.close();
        connection.close();
    }//end writeCachedRowSet()

    public static OracleCachedRowSet readCachedRowSet() throws Exception{
        //Read serialized CachedRowSet object from storage
        FileInputStream fileInputStream = new FileInputStream(CRS_FILE_LOC_TASK3);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        OracleCachedRowSet oracleCachedRowSet = (OracleCachedRowSet)objectInputStream.readObject();
        fileInputStream.close();
        objectInputStream.close();
        return oracleCachedRowSet;
    }//end readCachedRowSet()

    public static void bindDataSource(Context context, String dataSourceName)
            throws SQLException, NamingException {
        OracleDataSource oracleDataSource = new OracleDataSource();
        oracleDataSource.setUser(ORACLE_USER);
        oracleDataSource.setPassword(ORACLE_PASS);
        oracleDataSource.setDriverType("thin");
        oracleDataSource.setDatabaseName("orbis");
        oracleDataSource.setServerName("localhost");
        oracleDataSource.setPortNumber(1521);
        context.rebind (dataSourceName, oracleDataSource);
    }

}
