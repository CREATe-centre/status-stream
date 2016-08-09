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

import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import uk.ac.nottingham.create.stream.util.WordPressUtil;

public class Database {	
	
	private static final Logger logger = LogManager.getLogger(Database.class); 
	
	private final WordPressUtil.WpConfig wpConfig;
	
	private final DataSource dataSource;
	
	private final String dataTableName;
	
	private final String linkTableName;
	
	private final String analyticsTableName;
	
	public Database(WordPressUtil.WpConfig wpConfig, DataSource dataSource) 
	throws SQLException {
		this.wpConfig = wpConfig;
		this.dataSource = dataSource;
		this.dataTableName = wpConfig.dbPrefix + "twitter_data";
		this.linkTableName =  wpConfig.dbPrefix + "twitter_data_links";
		this.analyticsTableName = wpConfig.dbPrefix + "twitter_analytics";
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
				final PreparedStatement dataPs = connection
						.prepareStatement(
								"INSERT INTO "
										+ dataTableName
										+ " (user_id, twitter_user_id, event, data, created_at) "
										+ "VALUES ("
										+ "?, ?, ?, ?, ?"
										+ ")", Statement.RETURN_GENERATED_KEYS);
				try {
					dataPs.setLong(1, userID);
					dataPs.setLong(2, twitterUserID);
					dataPs.setString(3, event.name());
					dataPs.setString(4, data);
					dataPs.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
					logger.trace(new ParameterizedMessage("Database insert: {}", dataPs));
					dataPs.executeUpdate();
					ResultSet rs = dataPs.getGeneratedKeys();
					if(rs.next()) {
						long toID = rs.getLong(1);
						long fromID = findFromID(connection, userID, event, data);
						if(fromID != -1) {
							final PreparedStatement linkPs = connection
									.prepareStatement("INSERT INTO "
											+ linkTableName
											+ " (user_id, from_id, to_id) "
											+ "VALUES (?, ?, ?)");
							linkPs.setLong(1, userID);
							linkPs.setLong(2, fromID);
							linkPs.setLong(3, toID);
							logger.trace(new ParameterizedMessage("Database insert: {}", linkPs));
							linkPs.execute();
						}
					}										
				} finally {
					dataPs.close();
				}
			}
			finally {		
				connection.close();
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} 
	}
	
	private long findFromID(Connection conn, long userID, Event event, String data)
	throws 	TwitterException,
			SQLException,
			JSONException {
		Status status = null;
		switch (event) {
		case YOU_FAVOURITED:
		case YOU_UNFAVOURITED:
		case BLOCK:
		case UNBLOCK:
		case MESSAGE:
		case MESSAGE_DELETION:
		case UNFOLLOWED_YOU:
		case YOU_FOLLOWED:
		case YOU_UNFOLLOWED:
		case FOLLOWED_YOU:
		case RETWEETED_RETWEET:
			return -1;
		case TWEET:
		case STATUS:
		case MENTION:
			status = TwitterObjectFactory.createStatus(data);
			if(status.getInReplyToStatusId() != -1) {
				return findStatusByID(conn, userID, status.getInReplyToStatusId());
			} else {
				return -1;
			}
		case RETWEET:
		case FRIEND_RETWEET:
		case FRIEND_OF_FRIEND_RETWEET:
			status = TwitterObjectFactory.createStatus(data);
			return findStatusByID(conn, userID, status.getRetweetedStatus().getId());
		case FAVOURITED_RETWEET:
			status = TwitterObjectFactory.createStatus(new JSONObject(data).getString("status"));
			long linkedID = findStatusByID(conn, userID, status.getId());
			if(linkedID != -1) {
				return linkedID;
			} else {
				return findStatusByID(conn, userID, status.getRetweetedStatus().getId());
			}
		case FAVOURITED_YOU:
		case UNFAVOURITED_YOU:
			status = TwitterObjectFactory.createStatus(new JSONObject(data).getString("status"));
			return findStatusByID(conn, userID, status.getId());
		case QUOTED_TWEET:
			status = TwitterObjectFactory.createStatus(new JSONObject(data).getString("status"));
			return findStatusByID(conn, userID, status.getQuotedStatusId());
		}
		return -1;
	}
	
	private long findStatusByID(Connection conn, long userID, long id)
	throws SQLException { 
		Statement s = conn.createStatement();
		try
		{
			ResultSet rs = s.executeQuery("SELECT * FROM " + dataTableName 
					+ " WHERE user_id = " + userID 
					+ " ORDER BY created_at DESC");		
			while(rs.next()) {
				Status status = null;
				try {
					Event e = Event.valueOf(Event.class, rs.getString("event"));
					status = getStatus(e, rs.getString("data"));
				} catch(Exception e) {
					continue;
				}
				if(status != null && status.getId() == id) {
					return rs.getLong("ID");
				}
			}
		} finally {
			s.close();
		}
		return -1;
	}
	
	private Status getStatus(Event event, String data)
	throws  TwitterException,
			JSONException {
		switch (event) {
		case YOU_FAVOURITED:
		case YOU_UNFAVOURITED:
		case BLOCK:
		case UNBLOCK:
		case MESSAGE:
		case MESSAGE_DELETION:
		case UNFOLLOWED_YOU:
		case YOU_FOLLOWED:
		case YOU_UNFOLLOWED:
		case FOLLOWED_YOU:
		case RETWEETED_RETWEET:
			return null;
		case TWEET:
		case STATUS:
		case MENTION:
		case RETWEET:
		case FRIEND_RETWEET:
		case FRIEND_OF_FRIEND_RETWEET:
			return TwitterObjectFactory.createStatus(data);
		case FAVOURITED_RETWEET:
		case FAVOURITED_YOU:
		case UNFAVOURITED_YOU:
		case QUOTED_TWEET:
			return TwitterObjectFactory.createStatus(new JSONObject(data).getString("status"));
		default:
			return null;
		}
	}
	
	private void bootstrap() 
	throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			Statement s = conn.createStatement();
			if(!doesTableExist(dataTableName, conn)) {
				final String dataTableSql = "CREATE TABLE " + dataTableName + " (" 
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
				s.execute(dataTableSql);
			}
			if(!doesTableExist(linkTableName, conn)) {
				final String linkTableSql = "CREATE TABLE " + linkTableName + "("
						+ "ID BIGINT(20) unsigned NOT NULL AUTO_INCREMENT, "
						+ "user_id BIGINT(20) unsigned NOT NULL, "
						+ "from_id BIGINT(20) unsigned NOT NULL, "
						+ "to_id BIGINT(20) unsigned NOT NULL, "
						+ "PRIMARY KEY (ID), "
						+ "FOREIGN KEY (user_id) REFERENCES " 
						+ wpConfig.dbPrefix + "users(ID) "
						+ "ON DELETE CASCADE "
						+ "ON UPDATE CASCADE, "
						+ "FOREIGN KEY (from_id) REFERENCES " 
						+ dataTableName + "(ID) "
						+ "ON DELETE CASCADE "
						+ "ON UPDATE CASCADE, "
						+ "FOREIGN KEY (to_id) REFERENCES " 
						+ dataTableName + "(ID) "
						+ "ON DELETE CASCADE "
						+ "ON UPDATE CASCADE "
						+ ")";
				s.execute(linkTableSql);
			}
			if(!doesTableExist(analyticsTableName, conn)) {
				final String analyticsTableSql = "CREATE TABLE " 
						+ analyticsTableName + "("
						+ "ID BIGINT(20) unsigned NOT NULL AUTO_INCREMENT, "
						+ "status_id BIGINT(20) unsigned NOT NULL, "
						+ "impressions INT(20) unsigned NOT NULL, "
						+ "engagements INT(20) unsigned NOT NULL, "
						+ "retweets INT(20) unsigned NOT NULL, "
						+ "replies INT(20) unsigned NOT NULL, "
						+ "likes INT(20) unsigned NOT NULL, "
						+ "user_profile_clicks INT(20) unsigned NOT NULL, "
						+ "url_clicks INT(20) unsigned NOT NULL, "
						+ "hashtag_clicks INT(20) unsigned NOT NULL, "
						+ "detail_expands INT(20) unsigned NOT NULL, "
						+ "permalink_clicks INT(20) unsigned NOT NULL, "
						+ "app_opens INT(20) unsigned NOT NULL, "
						+ "app_installs INT(20) unsigned NOT NULL, "
						+ "follows INT(20) unsigned NOT NULL, "
						+ "email_tweet INT(20) unsigned NOT NULL, "
						+ "dial_phone INT(20) unsigned NOT NULL, "
						+ "media_views INT(20) unsigned NOT NULL, "
						+ "media_engagements INT(20) unsigned NOT NULL, "
						+ "promoted_impressions INT(20) unsigned NOT NULL, "
						+ "promoted_engagements INT(20) unsigned NOT NULL, "
						+ "promoted_retweets INT(20) unsigned NOT NULL, "
						+ "promoted_replies INT(20) unsigned NOT NULL, "
						+ "promoted_likes INT(20) unsigned NOT NULL, "
						+ "promoted_user_profile_clicks INT(20) unsigned NOT NULL, "
						+ "promoted_url_clicks INT(20) unsigned NOT NULL, "
						+ "promoted_hashtag_clicks INT(20) unsigned NOT NULL, "
						+ "promoted_detail_expands INT(20) unsigned NOT NULL, "
						+ "promoted_permalink_clicks INT(20) unsigned NOT NULL, "
						+ "promoted_app_opens INT(20) unsigned NOT NULL, "
						+ "promoted_app_installs INT(20) unsigned NOT NULL, "
						+ "promoted_follows INT(20) unsigned NOT NULL, "
						+ "promoted_email_tweet INT(20) unsigned NOT NULL, "
						+ "promoted_dial_phone INT(20) unsigned NOT NULL, "
						+ "promoted_media_views INT(20) unsigned NOT NULL, "
						+ "promoted_media_engagements INT(20) unsigned NOT NULL, "
						+ "PRIMARY KEY (ID), "
						+ "FOREIGN KEY (status_id) REFERENCES " 
						+ dataTableName + "(ID) "
						+ "ON DELETE CASCADE "
						+ "ON UPDATE CASCADE "
						+ ")";
				
				s.execute(analyticsTableSql);
			}
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
