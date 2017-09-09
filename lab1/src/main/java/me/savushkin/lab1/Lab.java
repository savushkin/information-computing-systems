package me.savushkin.lab1;

import java.sql.*;

public class Lab {
    public static void main(String[] args) {
        Connection conn = null;

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(
                    "jdbc:postgresql://pg:5432/ucheb","", "");
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM Н_ЦИКЛЫ_ДИСЦИПЛИН");
            while(rs.next()) {
                int count = rs.getInt(1);
                System.out.println("Result set is: "+count);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
