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
		
		Database database = new Database();
		
		final String tableName = "wp_twitter_data";		
		final String tableSQL = "CREATE TABLE " + tableName 
				+ " (id bigint(255) NOT NULL AUTO_INCREMENT, "
				+ "userid bigint(50) NOT NULL, "
				+ "event varchar(25) NOT NULL, "
				+ "JSONdata varchar(10000) NOT NULL, "
				+ "created_datetime varchar(50) NOT NULL, "
				+ "PRIMARY KEY (id))";
		
		database.createCustomTable(tableSQL, tableName);		
		GetStream stream = new GetStream(oauth.consumerKey, oauth.consumerSecret);		
		for(WordPressUtil.WpUser user : WordPressUtil.fetchUsers(conn, config.dbPrefix)) {
			stream.createStream(user.oauthToken, user.oauthTokenSecret);
		}				
	}
}
