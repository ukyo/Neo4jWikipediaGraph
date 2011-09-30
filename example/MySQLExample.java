package example;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.Connection;

public class MySQLExample {
	
	Connection connection;
	Statement statement;
	ResultSet resultSet;
	
	public MySQLExample() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			
			connection = DriverManager.getConnection("jdbc:mysql://localhost/test?user=root&password=ukyo&characterEncoding=UTF-8");
			
			statement = connection.createStatement();
			resultSet = statement.executeQuery("SELECT * FROM page WHERE page_id=3");
			Charset c = Charset.forName("UTF-8");
			
			while(resultSet.next()) {
				System.out.println(resultSet.getInt("page_id"));
				System.out.println(new String(resultSet.getBytes("page_title"), "UTF-8"));
			}
		} catch (SQLException e) {
			System.out.println("SQLException: " + e.getMessage());
			System.out.println("SQLState: " + e.getSQLState());
			System.out.println("VendorError: " + e.getErrorCode());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				resultSet = null;
			}
			
			if (statement != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				statement = null;
			}
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new MySQLExample();
	}

}
