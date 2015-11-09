package uk.ac.nottingham.createStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordPressUtil {
	
	public static class WpConfig {
		public final String host;
		public final String username;
		public final String password;
		public WpConfig(String host, String username, String password) {
			this.host = host;
			this.username = username;
			this.password = password;
		}
	}
	
	public static WpConfig parseWpConfig(File wpConfig)
	throws 
			FileNotFoundException,
			IOException {
		String host = null, username = null, password = null;
		BufferedReader r = new BufferedReader(
				new InputStreamReader(
						new BufferedInputStream(
								new FileInputStream(wpConfig))));
		try {
			String line;
			Pattern p = Pattern.compile(
					"^\\s*define\\s*\\(\\s*'(DB_NAME|DB_USER|DB_PASSWORD)'\\s*,"
					+ "\\s*'(.*)'\\s*\\)\\s*;\\s*.*$");
			while((line = r.readLine()) != null) {
				Matcher m = p.matcher(line);
				if(m.matches()) {
					switch(m.group(1)) {
					case "DB_NAME" : 
						host = m.group(2);
						break;
					case "DB_USER" :
						username = m.group(2);
						break;
					case "DB_PASSWORD" :
						password = m.group(2);
						break;
					}
				}
			}
		}
		finally {
			r.close();
		}
		return new WpConfig(host, username, password);
	}
	
	public static class OAuthSettings {
		public final String consumerKey;
		public final String consumerSecret;
		public OAuthSettings(String consumerKey, String consumerSecret) {
			this.consumerKey = consumerKey;
			this.consumerSecret = consumerSecret;
		}
	}
	
	public static OAuthSettings fetchOAuthSettings(Connection conn)
	throws SQLException {
		Statement s = conn.createStatement();
		
		try {
			ResultSet rs = s.executeQuery(
					"SELECT a.option_value as consumer_key, "
					+ "b.option_value as consumer_secret "
					+ "FROM wp_options a "
					+ "INNER JOIN wp_options b "
					+ "ON b.option_name = 'status-twitter-oauth-consumer-secret' "
					+ "WHERE a.option_name = 'status-twitter-oauth-consumer-key'");
			OAuthSettings oauth = null;
			if(rs.next()) {
				oauth = new OAuthSettings(
						rs.getString("consumer_key"),
						rs.getString("consumer_secret"));
			}
			rs.close();
			return oauth;
		} finally {
			s.close();
		}		
	}
	
	public static class WpUser {
		public final long id;
		public final String displayName;
		public final String oauthToken;
		public final String oauthTokenSecret;
		public WpUser(
				long id,
				String displayName,
				String oauthToken,
				String oauthTokenSecret) {
			this.id = id;
			this.displayName = displayName;
			this.oauthToken = oauthToken;
			this.oauthTokenSecret = oauthTokenSecret;
		}
	}
	
	public static List<WpUser> fetchUsers(Connection conn)
	throws SQLException {
		Statement s = conn.createStatement();
		ArrayList<WpUser> users = new ArrayList<WpUser>();
		try {
			ResultSet rs = s.executeQuery(
					"SELECT u.id, u.user_login, t.meta_value as token, s.meta_value as secret "
							+ "FROM wp_users u "
							+ "INNER JOIN wp_usermeta t on u.id = t.user_id "
							+ "AND t.meta_key = 'oauth_token' "
							+ "INNER JOIN wp_usermeta s on u.id = s.user_id "
							+ "AND s.meta_key = 'oauth_token_secret'");
			while(rs.next()) {
				users.add(new WpUser(
						rs.getLong("id"),
						rs.getString("user_login"),
						rs.getString("token"),
						rs.getString("secret")));
			}
			rs.close();
		} finally {
			s.close();
		}		
		return users;
	}
}
