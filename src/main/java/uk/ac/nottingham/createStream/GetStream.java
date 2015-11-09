package uk.ac.nottingham.createStream;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

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
	/**
	 * Enter the Twitter application consumer key and consumer secret. 
	 */
	final static String consumerKey = "xjUeizYakyKwnT3baI6LqwCTf";
	final static String consumerSecret = "4WoilzrO7CQi9gdgEWgJyqsFuwjmCe19jF8GpRa6FdGtOqodRq";
	
	/**
	 * Open user stream for specific authenticated user and start capturing event data to database.
	 * 
	 * @param accessToken
	 * @param accessTokenSecret
	 * @throws SQLException
	 * @throws IllegalStateException
	 * @throws TwitterException
	 */
	public void createStream(final String accessToken, final String accessTokenSecret) throws SQLException, IllegalStateException, TwitterException {		
		final Database database = new Database();
		final TwitterStream twitterStream = new TwitterStreamFactory(getConfiguration(accessToken, accessTokenSecret)).getInstance();	
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
					try {
						database.storeTwitterData(userId,
								Event.TWEET.toString(),								
								TwitterObjectFactory.getRawJSON(status).toString(), 
								(new Date()).toString());						
					} catch (SQLException e) {
					}					
				} 
			}

	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				try {
					database.storeTwitterData(statusDeletionNotice.getUserId(),
							Event.STATUS_DELETION.toString(),
							TwitterObjectFactory.getRawJSON(statusDeletionNotice).toString(),
							(new Date()).toString());
				} catch (SQLException e) {
				}        	
	           
	        }

	        public void onDeletionNotice(long directMessageId, long userId) {
				try {
					JSONObject json = new JSONObject();
		        	json.put("userId", userId);
		        	json.put("messageId", directMessageId);
					database.storeTwitterData(userId,
							Event.MESSAGE_DELETION.toString(),
							json.toString(),
							(new Date()).toString());
				} catch (SQLException e) {
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
	    					try {
	    						database.storeTwitterData(userId,
	    								Event.RETWEET.toString(),								
	    								TwitterObjectFactory.getRawJSON(status).toString(), 
	    								(new Date()).toString());						
	    					} catch (SQLException e) {
	    					}	                		
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
				try {
					database.storeTwitterData(favoritedStatus.getUser().getId(),
							Event.FAVOURITE.toString(),
							TwitterObjectFactory.getRawJSON(favoritedStatus).toString(),
							(new Date()).toString());
				} catch (SQLException e) {
				}	            
	        }

	        /** 
	         * 
	         */
	        public void onUnfavorite(User source, User target, Status unfavoritedStatus) {
				try {
					database.storeTwitterData(unfavoritedStatus.getUser().getId(),
							Event.UNFAVOURITE.toString(),
							TwitterObjectFactory.getRawJSON(unfavoritedStatus).toString(),
							(new Date()).toString());
				} catch (SQLException e) {
				}  
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
					database.storeTwitterData(userId,
							Event.FOLLOW.toString(),
							json.toString(),
							(new Date()).toString());
				} catch (SQLException e) {
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
					database.storeTwitterData(userId,
							Event.UNFOLLOW.toString(),
							json.toString(),
							(new Date()).toString());
				} catch (SQLException e) {
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
					database.storeTwitterData(userId,
							Event.MESSAGE.toString(),
							json.toString(),
							(new Date()).toString());
				} catch (SQLException e) {
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
					database.storeTwitterData(userId,
							Event.BLOCK.toString(),
							json.toString(),
							(new Date()).toString());
				} catch (SQLException e) {
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
					database.storeTwitterData(userId,
							Event.UNBLOCK.toString(),
							json.toString(),
							(new Date()).toString());
				} catch (SQLException e) {
				} catch (JSONException e) {
				}   
	        }

	        /** 
	         * 
	         */
	        public void onRetweetedRetweet(User source, User target, Status retweetedStatus) {
				try {
					database.storeTwitterData(userId,
							Event.RETWEETED_RETWEET.toString(),
							TwitterObjectFactory.getRawJSON(retweetedStatus).toString(), 
									(new Date()).toString());
				} catch (SQLException e) {
				}  
	        }

	        /** 
	         * 
	         */
	        public void onFavoritedRetweet(User source, User target, Status favoritedRetweet) {
				try {
					database.storeTwitterData(userId,
							Event.FAVOURITED_RETWEET.toString(),
							TwitterObjectFactory.getRawJSON(favoritedRetweet).toString(),
							(new Date()).toString());
				} catch (SQLException e) {
				}  
	        }

	        
	        /** 
	         * 
	         */
	        public void onQuotedTweet(User source, User target, Status quotingTweet) {
				try {
					database.storeTwitterData(userId,
							Event.QUOTED_TWEET.toString(),
							TwitterObjectFactory.getRawJSON(quotingTweet).toString(),
							(new Date()).toString());
				} catch (SQLException e) {
				}  
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
	private static Configuration getConfiguration(String accessToken, String accessTokenSecret) {
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
