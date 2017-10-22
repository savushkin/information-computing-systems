package ru.makhnovets.lab4;

import java.awt.geom.RectangularShape;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Lab {
    //33639
    //3. Не указывать импорт.
    //3. System.setProperty
    //1,6,7. DriverManager.getConnection(String url)
    //3. Statement, execute()
    //9. TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, DatabaseMetaData

    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "";
    private static String ORACLE_PASS = "";
    private static String ORACLE_URL = String.format(
            "jdbc:oracle:thin:%s/%s@localhost:1521:orbis",
            ORACLE_USER,
            ORACLE_PASS);
    private static String operatorsql = "CREATE TABLE операторы_связи (ид NUMERIC(3) CONSTRAINT ПК_ОС PRIMARY KEY, оператор VARCHAR(20))";
    private static String telefonsql = "CREATE TABLE номера_телефонов (ид_л NUMERIC(9) CONSTRAINT ВК_Л REFERENCES н_люди(ид), номер_телефона VARCHAR(20), дата_регистрации DATE, ид_ос NUMERIC(3) CONSTRAINT ВК_ОС REFERENCES операторы_связи(ид))";



    public static void main(String[] args) {
        java.sql.Connection connection = null;
        java.lang.System.setProperty("Djdbc.driver", ORACLE_DRIVER);
        try {
            connection = java.sql.DriverManager.getConnection(ORACLE_URL);
            java.sql.Statement statement = connection.createStatement(
                    java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_UPDATABLE);
            System.out.println("Create tables");
            createTables(statement);
            System.out.println("Insert data");
            insertData(statement);
            System.out.println("Select data");
            selectData(statement);

            String sql="SELECT * FROM операторы_связи WHERE ид=1";
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                if (resultSet.isFirst()) {
                    resultSet.updateString("оператор","new operator");
                    System.out.println("chance first row");
                }
                continue;
            }
            selectData(statement);
            statement.close();
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
    private static void selectData(Statement statement) throws SQLException {
        String sql="SELECT * FROM операторы_связи";
        Boolean isRetrieved = statement.execute(sql);
        System.out.println("Is data retrieved: " + isRetrieved);
        System.out.println("Displaying retrieved data:");
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            int id = resultSet.getInt("ид");
            String name = resultSet.getString("оператор");

            System.out.println("ID: " + id);
            System.out.println("Operator: " + name);
            //System.out.println("Specialty: " + specialty);
            //System.out.println("Salary: " + salary);
            System.out.println("===================");
        }
    }
    private static void createTables(Statement statement) throws SQLException {
        String operatorsql = "CREATE TABLE операторы_связи (ид NUMERIC(3) CONSTRAINT ПК_ОС PRIMARY KEY, оператор VARCHAR(20))";
        //String telefonsql = "CREATE TABLE номера_телефонов (ид_л NUMERIC(9) CONSTRAINT ВК_Л REFERENCES н_люди(ид), номер_телефона VARCHAR(20), дата_регистрации DATE, ид_ос NUMERIC(3) CONSTRAINT ВК_ОС REFERENCES операторы_связи(ид))";
        try {
            statement.execute("DROP TABLE операторы_связи CASCADE CONSTRAINTS");
        } catch (SQLException se) {
            //Игнорировать ошибку удаления таблицы
            if(se.getErrorCode()==942) {
                String msg = se.getMessage();
                System.out.println("Error drop table is fold: "+msg);
            }
        }
        //Создание таблицы операторы_связи
        if(statement.execute(operatorsql))
            System.out.println("Table operators create...");
    }
    private static void insertData(Statement statement) throws SQLException {
        //Загрузка данных в таблицу операторы_связи
        statement.execute("INSERT INTO операторы_связи VALUES(1,'Megafon')");
        statement.execute("INSERT INTO операторы_связи VALUES(2,'MTC')");
        statement.execute("INSERT INTO операторы_связи VALUES(3,'Beeline')");
        statement.execute("INSERT INTO операторы_связи VALUES(4,'SkyLink')");
    }
}
