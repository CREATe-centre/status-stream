package uk.ac.nottingham.create.stream;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import uk.ac.nottingham.create.stream.util.Action;
import uk.ac.nottingham.create.stream.util.WordPressUtil;

public class StreamFactory {
	
	private static final Logger logger = LogManager.getLogger(StreamFactory.class);  
	
	public static interface Callback {
		public void onShutdown();
	}	
	
	private final Database database;
	private final WordPressUtil.OAuthSettings oauth;
	
	public StreamFactory(Database database, WordPressUtil.OAuthSettings oauth) {
		this.database = database;
		this.oauth = oauth;
	}
	
	public void startStream(
			final WordPressUtil.WpUser wpUser,
			final Callback callback) 
	throws TwitterException {		
		
		final TwitterStream userStream = new TwitterStreamFactory(
				getConfiguration(wpUser.oauthToken, wpUser.oauthTokenSecret)).getInstance();	
		final TwitterStream friendStream = new TwitterStreamFactory(
				getConfiguration(wpUser.oauthToken, wpUser.oauthTokenSecret)).getInstance();
		
		final Set<Long> friends = Collections.synchronizedSet(new HashSet<Long>());
		
		final Action<Exception> onException = new Action<Exception>() {
			@Override
			public void doAction(Exception e) {
				logger.error(e.getMessage(), e);
				userStream.clearListeners();
				userStream.cleanUp();
				friendStream.clearListeners();
				friendStream.cleanUp();
		    	callback.onShutdown();				
			}
		};
		
		final long twitterID = userStream.getId();
		
		final FriendStreamListener friendListener = new FriendStreamListener(
				wpUser, twitterID, friends, database) {
			public void onException(Exception e) {
				onException.doAction(e);
			}
		};
				
		final UserStreamListener userListener = new UserStreamListener(
				wpUser, twitterID, friends, new Action<long[]>() {
					@Override
					public void doAction(long[] ids) {
						FilterQuery query = new FilterQuery();
						query.follow(ids);
						friendStream.filter(query);
					}
				}, database) {
			public void onException(Exception e) {
				onException.doAction(e);
			}
		};

		userStream.addListener(userListener);
		userStream.user();
		friendStream.addListener(friendListener);
	}
	
    
	private Configuration getConfiguration(String token, String tokenSecret) {
		return new ConfigurationBuilder()
				.setDebugEnabled(true)
				.setApplicationOnlyAuthEnabled(false)
				.setJSONStoreEnabled(true)
				.setOAuthConsumerKey(oauth.consumerKey)
				.setOAuthConsumerSecret(oauth.consumerSecret)
				.setOAuthAccessToken(token)
				.setOAuthAccessTokenSecret(tokenSecret).build();
	}
	
}
