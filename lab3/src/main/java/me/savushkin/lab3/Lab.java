package me.savushkin.lab3;

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
            Connection connection = null;
            try {
                LabHelper.registration(ORACLE_DRIVER);
                //LabHelper.registration(POSTGRES_DRIVER);

                connection = LabHelper.connection(ORACLE_URL, ORACLE_USER, ORACLE_PASS);
                //connection = LabHelper.connection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASS);

                switch (args[0]) {
                    case "task1": {
                        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

                        ResultSet resultSet = statement.executeQuery("SELECT * FROM операторы_связи");
                        System.out.printf("Row number %d; BFR is %s%n", resultSet.getRow(), resultSet.isBeforeFirst());
                        while (resultSet.next()) {
                            System.out.printf("Row number %d; First is %b", resultSet.getRow(), resultSet.isFirst());
                            System.out.printf(":\t%d", resultSet.getInt(1));
                            System.out.printf("\t%s%n", resultSet.getString(2));
                            System.out.printf("Row number %d; Last is %s%n", resultSet.getRow(), resultSet.isLast());
                        }
                        System.out.printf("Row number %d; ALR is %s%n", resultSet.getRow(), resultSet.isAfterLast());

                        break;
                    }
                    case "task2": {
                        Statement statement = connection.createStatement(
                                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

                        ResultSet resultSet = statement.executeQuery("SELECT * FROM операторы_связи");
                        resultSet.afterLast();
                        System.out.printf("Row number %d; BFR is %s%n", resultSet.getRow(), resultSet.isBeforeFirst());
                        resultSet.last();
                        do {
                            System.out.printf("Row number %d; First is %s", resultSet.getRow(), resultSet.isFirst());
                            System.out.printf(":\t%d", resultSet.getInt(1));
                            System.out.printf("\t%s%n", resultSet.getString(2));
                            System.out.printf("Row number %d; Last is %s%n", resultSet.getRow(), resultSet.isLast());
                        } while (resultSet.previous());
                        System.out.printf("Row number %d; ALR is %s%n", resultSet.getRow(), resultSet.isAfterLast());
                        break;
                    }
                    case "task3": {
                        Statement statement = connection.createStatement(
                                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                        ResultSet resultSet = statement.executeQuery("SELECT ИД, ОПЕРАТОР FROM операторы_связи");
                        while (resultSet.next()) {
                            System.out.printf("%d\t%s%n", resultSet.getInt(1), resultSet.getString(2));
                            if(resultSet.getString(2).equals("МТС")) {
                                resultSet.updateString(2, "TELE2");
                                resultSet.updateRow();
                            }
                        }
                        resultSet = statement.executeQuery("SELECT * FROM операторы_связи");
                        LabHelper.printResultSet(resultSet);
                        break;
                    }
                    case "task4": {
                        Statement statement = connection.createStatement(
                                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                        ResultSet resultSet = statement.executeQuery("SELECT ИД, ОПЕРАТОР FROM операторы_связи");
                        while (resultSet.next()) {
                            System.out.printf("%d\t%s%n", resultSet.getInt(1), resultSet.getString(2));
                            if(resultSet.getString(2).equals("SkyLink2"))
                                resultSet.deleteRow();
                        }
                        resultSet.moveToInsertRow();
                        resultSet.updateInt(1, 5);
                        resultSet.updateString(2, "TELE2");
                        resultSet.insertRow();
                        //rs = st.executeQuery("SELECT * FROM операторы_связи");
                        resultSet.beforeFirst();
                        LabHelper.printResultSet(resultSet);
                        break;
                    }
                    case "task5": {

                        break;
                    }
                    case "task6": {

                        break;
                    }
                    case "task7": {

                        break;
                    }
                    default:
                        System.err.println("Undefined argument");
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
