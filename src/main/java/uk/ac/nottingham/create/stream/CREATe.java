package uk.ac.nottingham.create.stream;

import java.io.File;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import twitter4j.TwitterException;
import uk.ac.nottingham.create.stream.util.WordPressUtil;

public class CREATe {

	private static final Logger logger = LogManager.getLogger(CREATe.class);
	
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
			
									
			final Set<Long> streams = Collections.synchronizedSet(new HashSet<Long>());
			final Set<Long> removedStreams = Collections.synchronizedSet(new HashSet<Long>());
			
			final ScheduledExecutorService scheduler =
					Executors.newSingleThreadScheduledExecutor();
			
			final Runnable monitor = new Runnable() {
				@Override
				public void run() {
					
					try {
						final Connection conn = ds.getConnection();
						try {
							StreamFactory stream = new StreamFactory(db, oauth);			
							for(final WordPressUtil.WpUser user : 
									WordPressUtil.fetchUsers(conn, config.dbPrefix)) {
								if(streams.contains(user.id) || removedStreams.contains(user.id))
									continue;
								logger.debug("Starting stream for user \"" + user.id + "\"");
								try {
									stream.startStream(user, new StreamFactory.Callback() {
										@Override
										public void onShutdown() {
											logger.debug("Removing stream for user \"" + user.id + "\"");
											removedStreams.add(user.id);
											streams.remove(user.id);							
										}
									});
								} catch(TwitterException e) {
									logger.error("Couldn't start stream for user \"" + user.id + "\"", e);
								}
								streams.add(user.id); 
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
