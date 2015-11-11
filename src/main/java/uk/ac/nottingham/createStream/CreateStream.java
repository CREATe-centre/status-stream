package uk.ac.nottingham.createStream;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public class CreateStream {

	public static void main(String[] args) throws Exception {

		WordPressUtil.WpConfig config = 
				WordPressUtil.parseWpConfig(new File(args[0]));
		
		DBConnect.connectionString = "jdbc:mysql://" + config.host + "/" + config.name;
		DBConnect.dbName = config.username;
		DBConnect.passsord = config.password;
		
		Class.forName("com.mysql.jdbc.Driver");
		Connection conn = DriverManager.getConnection(
				"jdbc:mysql://" + config.host + "/" + config.name,
				config.username,
				config.password);
		
		WordPressUtil.OAuthSettings oauth =
				WordPressUtil.fetchOAuthSettings(conn, config.dbPrefix);
		
		GetStream stream = new GetStream(oauth.consumerKey, oauth.consumerSecret);
		
		for(WordPressUtil.WpUser user : WordPressUtil.fetchUsers(conn, config.dbPrefix)) {
			stream.createStream(
					user.oauthToken,
					user.oauthTokenSecret);
		}
		
		
	}
}
