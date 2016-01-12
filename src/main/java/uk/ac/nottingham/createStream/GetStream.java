package uk.ac.nottingham.createStream;

import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.lang3.ArrayUtils;

import twitter4j.DirectMessage;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class GetStream {
	
	private final String consumerKey;
	private final String consumerSecret;
	private final Database database;
	
	public GetStream(
			final Database database, 
			final WordPressUtil.OAuthSettings oauth) {
		this.database = database;
		this.consumerKey = oauth.consumerKey;
		this.consumerSecret = oauth.consumerSecret;
	}
	
	/**
	 * Open user stream for specific authenticated user and start capturing event data to database.
	 * 
	 * @param accessToken
	 * @param accessTokenSecret
	 * @throws SQLException
	 * @throws IllegalStateException
	 * @throws TwitterException
	 */
	public void createStream(final WordPressUtil.WpUser wpUser) 
	throws 
			SQLException, 
			IllegalStateException, 
			TwitterException {		
		
		final TwitterStream twitterStream = new TwitterStreamFactory(
				getConfiguration(wpUser.oauthToken, wpUser.oauthTokenSecret)).getInstance();	
		final Long userId =  twitterStream.getId();
		final ArrayList<Long> friendIdsList = new ArrayList<Long>(); 
		
		/*************************
		 * User Stream Listener
		 *************************/	
		UserStreamListener userListener = new UserStreamListener() {			
			/**  
			 * Capture status changes for authenticated user and write to data store.
			 *  @param status  
			 */
			public void onStatus(Status status) {	
				Long currentId = status.getUser().getId();
				if(userId.equals(currentId)) {
					database.store(
							wpUser.id,
							userId,
							Event.TWEET,
							TwitterObjectFactory.getRawJSON(status).toString());
				} 
			}

	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				database.store(wpUser.id,
						statusDeletionNotice.getUserId(),
						Event.STATUS_DELETION,
						TwitterObjectFactory.getRawJSON(statusDeletionNotice).toString());
	        }

	        public void onDeletionNotice(long directMessageId, long userId) {
				try {
					JSONObject json = new JSONObject();
		        	json.put("userId", userId);
		        	json.put("messageId", directMessageId);
					database.store(
							wpUser.id,
							userId,
							Event.MESSAGE_DELETION,
							json.toString());
				} catch (JSONException e) {
				}	
	            
	        }

	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onScrubGeo(long userId, long upToStatusId) {}
	        public void onStallWarning(StallWarning warning) {}

	        /** 
	         * Get the users friend list and invoke the Status listener to capture friends status changes
	         * @return 
	         * 
	         */
	        public void onFriendList(long[] friendIds) {
	            for (long friendId : friendIds) {
	                friendIdsList.add(friendId);
	            }
	    		StatusListener statusListner = new StatusListener() {
	                public void onStatus(Status status) {
	                	if(status.isRetweet()) {	                		
	    					database.store(wpUser.id,
	    							userId,
	    							Event.RETWEET,
	    							TwitterObjectFactory.getRawJSON(status).toString());						            		
	                	}	                	
	                }
	                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) { }
	                public void onTrackLimitationNotice(int numberOfLimitedStatuses) { }
	                public void onScrubGeo(long userId, long upToStatusId) { }
	                public void onStallWarning(StallWarning warning) { }
	                public void onException(Exception ex) {
	                }                    
	            };
	            invokeStatusListener(twitterStream, statusListner, friendIdsList); 	                   
	        }	        
	        
	        /** 
	         * Captures when authenticated user favourites a tweet or when a tweet 
	         * is 'favourited' by  authenticated users follower.
	         */
	        public void onFavorite(User source, User target, Status favoritedStatus) {	        	
				database.store(
						wpUser.id,
						favoritedStatus.getUser().getId(),
						Event.FAVOURITE,
						TwitterObjectFactory.getRawJSON(favoritedStatus).toString());        
	        }

	        /** 
	         * 
	         */
	        public void onUnfavorite(User source, User target, Status unfavoritedStatus) {
				database.store(
						wpUser.id,
						unfavoritedStatus.getUser().getId(),
						Event.UNFAVOURITE,
						TwitterObjectFactory.getRawJSON(unfavoritedStatus).toString());
	        }
	        
	        /** 
	         * Captures when authenticated user follows someone or is followed.
	         */
	        public void onFollow(User source, User followedUser) {     
				try {
					JSONObject json = new JSONObject();
		        	json.put("sourceId", source.getId());
		        	json.put("sourceName", source.getScreenName());
		        	json.put("followedUserId", followedUser.getId());	
		        	json.put("followedUserName", followedUser.getScreenName());
					database.store(
							wpUser.id,
							userId,
							Event.FOLLOW,
							json.toString());
				} catch (JSONException e) {
				}	        	
	                       
	        }
	        
	        
	        /** 
	         * Captures when authenticated user unfollows someone but does NOT capture
	         * when a follower unfollows authenticated user.
	         */
	        public void onUnfollow(User source, User unfollowedUser) {
				try {
					JSONObject json = new JSONObject();
		        	json.put("sourceId", source.getId());
		        	json.put("sourceName", source.getScreenName());
		        	json.put("unfollowedUserId", unfollowedUser.getId());	
		        	json.put("unfollowedUserName", unfollowedUser.getScreenName());
					database.store(
							wpUser.id,
							userId,
							Event.UNFOLLOW,
							json.toString());
				} catch (JSONException e) {
				}   	        	        	
	            
	        }

	        /** 
	         * Captures when authenticated user send or receives a direct message.
	         * 
	         */
	        public void onDirectMessage(DirectMessage directMessage) {
				try {
					JSONObject json = new JSONObject();
		        	json.put("messageId", directMessage.getId());
		        	json.put("messageSenderId", directMessage.getSenderId());
		        	json.put("messageRecipientId", directMessage.getRecipientId());	
		        	json.put("messageText", directMessage.getText());
					database.store(wpUser.id,
							userId,
							Event.MESSAGE,
							json.toString());
				} catch (JSONException e) {
				}               
	        }

	        public void onUserListMemberAddition(User addedMember, User listOwner, UserList list) {}
	        public void onUserListMemberDeletion(User deletedMember, User listOwner, UserList list) {}
	        public void onUserListSubscription(User subscriber, User listOwner, UserList list) {}
	        public void onUserListUnsubscription(User subscriber, User listOwner, UserList list) {}
	        public void onUserListCreation(User listOwner, UserList list) {}
	        public void onUserListUpdate(User listOwner, UserList list) {}
	        public void onUserListDeletion(User listOwner, UserList list) {}
	        public void onUserProfileUpdate(User updatedUser) {}
	        public void onUserDeletion(long deletedUser) {}
	        public void onUserSuspension(long suspendedUser) {}

	        public void onBlock(User source, User blockedUser) {
				try {
					JSONObject json = new JSONObject();
		        	json.put("sourceId", source.getId());
		        	json.put("sourceName", source.getScreenName());
		        	json.put("blockedUserId", blockedUser.getId());	
		        	json.put("blockedUserName", blockedUser.getScreenName());
					database.store(
							wpUser.id,
							userId,
							Event.BLOCK,
							json.toString());
				} catch (JSONException e) {
				}   

	        }

	        public void onUnblock(User source, User unblockedUser) {
				try {
					JSONObject json = new JSONObject();
		        	json.put("sourceId", source.getId());
		        	json.put("sourceName", source.getScreenName());
		        	json.put("unblockedUserId", unblockedUser.getId());	
		        	json.put("unblockedUserName", unblockedUser.getScreenName());
					database.store(
							wpUser.id,
							userId,
							Event.UNBLOCK,
							json.toString());
				} catch (JSONException e) {
				}   
	        }

	        /** 
	         * 
	         */
	        public void onRetweetedRetweet(User source, User target, Status retweetedStatus) {
					database.store(
							wpUser.id,
							userId,
							Event.RETWEETED_RETWEET,
							TwitterObjectFactory.getRawJSON(retweetedStatus).toString());
	        }

	        /** 
	         * 
	         */
	        public void onFavoritedRetweet(User source, User target, Status favoritedRetweet) {
					database.store(
							wpUser.id,
							userId,
							Event.FAVOURITED_RETWEET,
							TwitterObjectFactory.getRawJSON(favoritedRetweet).toString());
	        }

	        
	        /** 
	         * 
	         */
	        public void onQuotedTweet(User source, User target, Status quotingTweet) {
					database.store(
							wpUser.id,
							userId,
							Event.QUOTED_TWEET,
							TwitterObjectFactory.getRawJSON(quotingTweet).toString()); 
	        }

			public void onException(Exception ex) {	
			}			
		};

		twitterStream.addListener(userListener);
		twitterStream.user();	

	}
	
    /**
     * Start the Status listener and filter for authenticated users friends.
     * This is used to capture retweets.   
     * 
     * @param twitterStream
     * @param statusListner
     * @param friendIdsList
     */
    public void invokeStatusListener(TwitterStream twitterStream, StatusListener statusListner, ArrayList<Long> friendIdsList) {
        twitterStream.addListener(statusListner);    
        FilterQuery query = new FilterQuery();   
        long[] friendIds = ArrayUtils.toPrimitive(new Long[friendIdsList.size()]); 
        query.follow(friendIds);
        twitterStream.filter(query);        	
    }
	
	
	/**
	 * Authorise Twitter OAuth credentials
	 * 
	 *  @param accessToken
	 *  @param accessTokenSecret
	 *  @return
	 */
	private Configuration getConfiguration(String accessToken, String accessTokenSecret) {
		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
		return configurationBuilder
				.setDebugEnabled(true)
				.setJSONStoreEnabled(true)
				.setOAuthConsumerKey(consumerKey)
				.setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken)
				.setOAuthAccessTokenSecret(accessTokenSecret).build();
	} 		
}
