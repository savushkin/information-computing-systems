package ru.makhnovets.lab4;

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

    public static void main(String[] args) {
        java.sql.Connection connection = null;
        java.lang.System.setProperty("Djdbc.driver", ORACLE_DRIVER);
        try {
            connection = java.sql.DriverManager.getConnection(ORACLE_URL);
            java.sql.Statement statement = connection.createStatement(
                    java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                    java.sql.ResultSet.CONCUR_UPDATABLE);
            statement.execute("SELECT COUNT(*) FROM all_tables");
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
}
