package uk.ac.nottingham.createStream;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

public class Database {	
	
	private final WordPressUtil.WpConfig wpConfig;
	
	private final DataSource dataSource;
	
	public Database(WordPressUtil.WpConfig wpConfig, DataSource dataSource) 
	throws SQLException {
		this.wpConfig = wpConfig;
		this.dataSource = dataSource;
		bootstrap();
	}
	
	private void bootstrap() 
	throws SQLException {
		final String tableName = wpConfig.dbPrefix + "twitter_data";		
		final String tableSQL = "CREATE TABLE " + tableName 
				+ " (id bigint(255) NOT NULL AUTO_INCREMENT, "
				+ "user_id BIGINT(20) NOT NULL, "
				+ "twitter_user_id BIGINT(50) NOT NULL, "
				+ "event varchar(25) NOT NULL, "
				+ "JSONdata text NOT NULL, "
				+ "created_datetime varchar(50) NOT NULL, "
				+ "PRIMARY KEY (id))";
		
		createCustomTable(tableSQL, tableName);
	}
	
	/**
	 * Create a connection to the Wordpress MySql database
	 * @return connection
	 * @throws SQLException 
	 */
	public Connection getConnection() 
	throws SQLException {
		return dataSource.getConnection();
	}
	
	/**
	 * Stores data from the Twitter streaming API in raw JSON format
	 *  
	 * @param userId
	 * @param JSONData
	 * @param timestamp
	 * @throws SQLException
	 */
	public void storeTwitterData(
			long userID, long twitterUserId, String event, String JSONData, String timestamp) throws SQLException {		
		Connection connection = getConnection();
		try {
			final PreparedStatement preparedStatement = connection
					.prepareStatement(
							"INSERT INTO "
									+ "wp_twitter_data"
									+ " (user_id, twitter_user_id, event, JSONdata, created_datetime) "
									+ "VALUES ("
									+ "?, ?, ?, ?"
									+ ")");
			
			preparedStatement.setLong(1, userID);
			preparedStatement.setLong(2, twitterUserId);
			preparedStatement.setString(3, event);
			preparedStatement.setString(4, JSONData);
			preparedStatement.setString(5, timestamp);
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
