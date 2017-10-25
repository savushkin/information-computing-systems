package ru.makhnovets.lab4;

/*import java.awt.geom.RectangularShape;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;*/

public class Lab {
    //33639
    //3. Не указывать импорт.
    //3. System.setProperty
    //1,6,7. DriverManager.getConnection(String url)
    //3. Statement, execute()
    //9. TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, DatabaseMetaData

    private static String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String ORACLE_USER = "s182119";
    private static String ORACLE_PASS = "fax573";
    private static String ORACLE_URL = String.format(
            "jdbc:oracle:thin:%s/%s@localhost:1521:orbis",
            ORACLE_USER,
            ORACLE_PASS);

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
            System.out.println("Change data");
            changeData(statement);
            selectData(statement);
            getDatabaseMetaData(connection);
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

    //select data
    private static void selectData(java.sql.Statement statement) throws java.sql.SQLException {
        String sql="SELECT * FROM apartment";
        Boolean isRetrieved = statement.execute(sql);
        System.out.println("Is data retrieved: " + isRetrieved);
        System.out.println("Displaying retrieved data:");
        java.sql.ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            int price = resultSet.getInt("price");
            String metro = resultSet.getString("metro");
            String phone = resultSet.getString("phone");
            String email = resultSet.getString("email");

            System.out.println("id: " + id);
            System.out.println("name: " + name);
            System.out.println("price: " + price);
            System.out.println("metro: " + metro);
            System.out.println("phone: " + phone);
            System.out.println("email: " + email);
            System.out.println("===================");
        }
    }
    //create table
    private static void createTables(java.sql.Statement statement) throws java.sql.SQLException {
        String apartmentsql = "CREATE TABLE apartment (id NUMBER PRIMARY KEY, name VARCHAR(50), price NUMBER, metro VARCHAR(50), phone VARCHAR(50), email VARCHAR(50))";
        try {
            statement.execute("DROP TABLE apartment CASCADE CONSTRAINTS");
        } catch (java.sql.SQLException se) {
            //Игнорировать ошибку удаления таблицы
            if(se.getErrorCode()==942) {
                String msg = se.getMessage();
                System.out.println("Error drop table is fold: "+msg);
            }
        }
        //Создание таблицы операторы_связи
        if(statement.execute(apartmentsql))
            System.out.println("Table operators create...");
    }
    //insertData
    private static void insertData(java.sql.Statement statement) throws java.sql.SQLException {
        //Загрузка данных в таблицу операторы_связи
        statement.execute("INSERT INTO apartment VALUES(1, 'Nina', 30000, 'Petrogradskaya', '+79423182475', 'kakoi-to@email.com')");
        statement.execute("INSERT INTO apartment VALUES(2, 'Nina', 45000, 'Petrogradskaya', '+79423182475', 'kakoi-to@email.com')");
        statement.execute("INSERT INTO apartment VALUES(3, 'Daniil', 30000, 'Gorkovskaya', '+79812532345', 'kakoi-to-eche@email.com')");
        statement.execute("INSERT INTO apartment VALUES(4, 'Viktoria', 10000, 'Kupchino', '+78126669969', 'kupchino@email.com')");
        statement.execute("INSERT INTO apartment VALUES(5, 'Viktoria', 30000, 'Kupchino', '+78129996696', 'kupchino@email.com')");
    }

    //step 5, change data with updateble ResultSet
    private static void changeData(java.sql.Statement statement) throws java.sql.SQLException {
        String sql="SELECT email,name FROM apartment";
        java.sql.ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            String name = resultSet.getString("name");
            if (name.equalsIgnoreCase("Daniil")) {
                resultSet.updateString("email","daniil@google.com");
                resultSet.updateRow();
                System.out.println("change row");
                break;
            }
            continue;
        }

        }

        //step 5, DatabaseMetaData
        private static void getDatabaseMetaData(java.sql.Connection connection) throws java.sql.SQLException {
            java.sql.DatabaseMetaData databaseMetaData = connection.getMetaData();
            String productName = databaseMetaData.getDatabaseProductName();
            int productMajorVersion = databaseMetaData.getDatabaseMajorVersion();
            System .out.println("Database name: "+productName+" ; major version: "+productMajorVersion);
            // Набор данных поддерживаемых типов
            java.sql.ResultSet resultSet = databaseMetaData.getTypeInfo();
            System.out.println("A description of all the data types supported by this database:");
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }
            if (databaseMetaData.allProceduresAreCallable()){
                System.out.println("The current user can call all the procedures returned by the method getProcedures");

            }else {System.out.println("The current user can't call all the procedures returned by the method getProcedures");}

        }
    }
