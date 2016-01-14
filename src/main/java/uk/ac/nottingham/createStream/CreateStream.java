package uk.ac.nottingham.createStream;

import java.io.File;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import twitter4j.TwitterStream;

public class CreateStream {

	private static final Logger logger = LogManager.getLogger(CreateStream.class);
	
	public static void main(String[] args) {
		try {
			final WordPressUtil.WpConfig config = 
					WordPressUtil.parseWpConfig(new File(args[0]));
			
			final ComboPooledDataSource ds = new ComboPooledDataSource();
			ds.setDriverClass("com.mysql.jdbc.Driver");
			ds.setJdbcUrl("jdbc:mysql://" + config.host + "/" + config.name + "?autoReconnect=true");
			ds.setUser(config.username);
			ds.setPassword(config.password);
			ds.setMaxStatements(100);
			ds.setTestConnectionOnCheckout(true);
			
			final Database db = new Database(config, ds);
			
			Connection conn = ds.getConnection();
			final WordPressUtil.OAuthSettings oauth =
					WordPressUtil.fetchOAuthSettings(conn, config.dbPrefix);
			conn.close();
			
									
			final Map<Long, TwitterStream> streams = 
					Collections.synchronizedMap(
							new HashMap<Long, TwitterStream>());
			
			final ScheduledExecutorService scheduler =
					Executors.newSingleThreadScheduledExecutor();
			
			final Runnable monitor = new Runnable() {
				@Override
				public void run() {
					
					try {
						final Connection conn = ds.getConnection();
						try {
							GetStream stream = new GetStream(db, oauth);			
							for(final WordPressUtil.WpUser user : 
									WordPressUtil.fetchUsers(conn, config.dbPrefix)) {
								if(streams.containsKey(user.id))
									continue;
								logger.debug("Starting stream for user \"" + user.id + "\"");
								streams.put(user.id, stream.createStream(user, new GetStream.StreamCallback() {
									@Override
									public void onShutdown() {
										logger.debug("Removing stream for user \"" + user.id + "\"");
										streams.remove(user.id);							
									}
								}));
							}
						} finally {
							conn.close();
						}
					} catch(Exception e) {
						logger.error(e.getMessage(), e);
						System.exit(1);
					}
				}
			};
			
			scheduler.scheduleAtFixedRate(monitor, 0, 1, TimeUnit.MINUTES);
						
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			System.exit(1);
		}
	}
}
