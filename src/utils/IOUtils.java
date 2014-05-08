package utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class IOUtils {
    
    public static void closeQuitely(ResultSet resultSet) {
        if (null != resultSet) {
            try {
                resultSet.close();
            }catch (Exception e) {}
        }
    }
    
    public static void closeQuitely(Statement statement) {
        if (null != statement) {
            try {
                statement.close();
            }catch (Exception e) {}
        }
    }
    
    public static void closeQuitely(Connection connection) {
        if (null != connection) {
            try {
                connection.close();
            }catch (Exception e) {}
        }
    }
}
