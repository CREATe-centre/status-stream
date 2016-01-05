package uk.ac.nottingham.createStream;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Database {	
	
	/**
	 * Create a connection to the Wordpress MySql database
	 * @return connection
	 */
	public Connection getConnection() {
			try {
				Class.forName("com.mysql.jdbc.Driver");
				return DriverManager.getConnection(	DBConnect.connectionString, DBConnect.dbName, DBConnect.passsord);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		return null;
	}
	
	/**
	 * Stores data from the Twitter streaming API in raw JSON format
	 *  
	 * @param userId
	 * @param JSONData
	 * @param timestamp
	 * @throws SQLException
	 */
	public void storeTwitterData(long userId, String event, String JSONData, String timestamp) throws SQLException {		
		Connection connection = getConnection();
		try {
			final PreparedStatement preparedStatement = connection
					.prepareStatement(
							"INSERT INTO "
									+ "wp_twitter_data"
									+ " (userid, event, JSONdata, created_datetime) "
									+ "VALUES ("
									+ "?, ?, ?, ?"
									+ ")");
			
			preparedStatement.setLong(1, userId);
			preparedStatement.setString(2, event);
			preparedStatement.setString(3, JSONData);
			preparedStatement.setString(4, timestamp);
			preparedStatement.execute();
			System.err.println("DB Insert " + preparedStatement.toString())  ;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {		
			connection.close();
		}			
	}
	
	/**
	 * Create a custom table in the wordpress database. Checks if the supplied table 
	 * name is present in the database. if not then it is created.
	 *  
	 * @param createSQL
	 * @param tableName
	 * @throws SQLException
	 */
	public void createCustomTable(String createSQL, String tableName) throws SQLException {
		if (!doesTableExist(tableName)) {			
			Connection connection = getConnection();
			try {				
				Statement statement = connection.createStatement();
				statement.execute(createSQL);				
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {			
				connection.close();
			}			
		}
	}

	
	/**
	 * Checks if the supplied table name is present in the database.
	 * 
	 * @param tableName
	 * @return boolean
	 * @throws SQLException
	 */
	private boolean doesTableExist(String tableName) throws SQLException {
		Connection connection = getConnection();
		DatabaseMetaData dbm = connection.getMetaData();
		ResultSet table = dbm.getTables(null, null, tableName, null);
		return table.next() ? true : false;
	}
}
