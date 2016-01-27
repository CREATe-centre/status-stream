package uk.ac.nottingham.create.stream;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import uk.ac.nottingham.create.stream.util.WordPressUtil;

public class Database {	
	
	private static final Logger logger = LogManager.getLogger(Database.class); 
	
	private final WordPressUtil.WpConfig wpConfig;
	
	private final DataSource dataSource;
	
	public Database(WordPressUtil.WpConfig wpConfig, DataSource dataSource) 
	throws SQLException {
		this.wpConfig = wpConfig;
		this.dataSource = dataSource;
		bootstrap();
	}
	
	/**
	 * Stores data from the Twitter streaming API in raw JSON format
	 *  
	 * @param userID
	 * @param twitterUserID
	 * @param event
	 * @param data
	 * @throws SQLException
	 */
	public void store(
			long userID, 
			long twitterUserID, 
			Event event, 
			String data) {	
		logger.debug(new ParameterizedMessage("Logging event '{}' for user id '{}'", event, userID));
		try {
			Connection connection = dataSource.getConnection();
			try {
				final PreparedStatement preparedStatement = connection
						.prepareStatement(
								"INSERT INTO "
										+ "wp_twitter_data"
										+ " (user_id, twitter_user_id, event, data, created_at) "
										+ "VALUES ("
										+ "?, ?, ?, ?, ?"
										+ ")");
				try {
					preparedStatement.setLong(1, userID);
					preparedStatement.setLong(2, twitterUserID);
					preparedStatement.setString(3, event.name());
					preparedStatement.setString(4, data);
					preparedStatement.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
					logger.trace(new ParameterizedMessage("Database insert: {}", preparedStatement));
					preparedStatement.execute();
				} finally {
					preparedStatement.close();
				}
			}
			finally {		
				connection.close();
			}
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		} 
	}
	
	private void bootstrap() 
	throws SQLException {
		final String tableName = wpConfig.dbPrefix + "twitter_data";		
		Connection conn = dataSource.getConnection();
		try {
			if(doesTableExist(tableName, conn))
				return;
			final String sql = "CREATE TABLE " + tableName + " (" 
					+ "ID BIGINT(20) unsigned NOT NULL AUTO_INCREMENT, "
					+ "user_id BIGINT(20) unsigned NOT NULL, "
					+ "twitter_user_id VARCHAR(255) NOT NULL, "
					+ "event VARCHAR(25) NOT NULL, "
					+ "data TEXT NOT NULL, "
					+ "created_at DATETIME NOT NULL, "
					+ "PRIMARY KEY (ID), "
					+ "FOREIGN KEY (user_id) REFERENCES " 
					+ wpConfig.dbPrefix + "users(ID) "
					+ "ON DELETE CASCADE "
					+ "ON UPDATE CASCADE "
					+ ")";
			
			Statement s = conn.createStatement();
			s.execute(sql);
			s.close();			
		}
		finally {
			conn.close();
		}
	}
	
	/**
	 * Checks if the supplied table name is present in the database.
	 * 
	 * @param tableName
	 * @param conn
	 * @return boolean
	 * @throws SQLException
	 */
	private boolean doesTableExist(String tableName, Connection conn) 
	throws SQLException {
		DatabaseMetaData dbm = conn.getMetaData();
		ResultSet table = dbm.getTables(null, null, tableName, null);
		return table.next() ? true : false;
	}
}
