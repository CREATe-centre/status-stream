package uk.ac.nottingham.createStream;

import java.io.File;
import java.sql.Connection;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class CreateStream {

	public static void main(String[] args) 
	throws Exception {

		WordPressUtil.WpConfig config = 
				WordPressUtil.parseWpConfig(new File(args[0]));
		
		ComboPooledDataSource ds = new ComboPooledDataSource();
		ds.setDriverClass("com.mysql.jdbc.Driver");
		ds.setJdbcUrl("jdbc:mysql://" + config.host + "/" + config.name);
		ds.setUser(config.username);
		ds.setPassword(config.password);
		ds.setMaxStatements(100);
		
		Database db = new Database(config, ds);
		Connection conn = db.getConnection();
		
		try {
			WordPressUtil.OAuthSettings oauth =
					WordPressUtil.fetchOAuthSettings(conn, config.dbPrefix);
			GetStream stream = new GetStream(db, oauth);			
			for(WordPressUtil.WpUser user : WordPressUtil.fetchUsers(conn, config.dbPrefix)) {
				stream.createStream(user);
			}
		}
		finally {
			conn.close();
		}		
	}
}
