package ru.makhnovets.lab9;

public class S182119 {
    /*
    На зачетном занятии необходимо создать java-класс, включающий в себя следующие обязательные действия:
    1. Восстановить объект типа CachedRowSet из файла, сохраненного при выполнении задания №8 в домашнем каталоге студента, ID которого совпадает с номером варианта, выданного преподавателем.
    2. Вывести на экран данные полученного объекта типа CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
    3. Создать в базе данных таблицу с соответствующим количеством столбцов и заполнить ее данными объекта типа CachedRowSet. Типы данных объекта CachedRowSet и столбцов таблицы должны совпадать.
    4. Обновить данные в таблице и выдать их используя тот же самый объект CachedRowSet в виде таблицы с названиями столбцов и указанием типа данных каждого столбца, для чего необходимо получить объект типа ResultSetMetaData и воспользоваться его методами.
    Название класса должно совпадать с идентификатором студента, выполняющего задание. Класс и исходный файл с расширением .java должны находится в корне домашнего каталога студента, выполняющего задание.
    Каждый шаг выполнения задания необходимо сопровождать выводом соответствующий результатов, демонстрирующих правильность его выполнения. Все шаги должны быть снабжены подробными комментариями.
    На защите необходимо знать ВСЁ!!!
    Время выполнения контрольного задания – 1 час 20 минут.
    */
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



