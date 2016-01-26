package uk.ac.nottingham.createStream;

import java.sql.SQLException;
import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.DirectMessage;
import twitter4j.FilterQuery;
import twitter4j.JSONObject;
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
import twitter4j.UserMentionEntity;
import twitter4j.UserStreamListener;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import static uk.ac.nottingham.createStream.Event.*;

public class GetStream {
	
	public static interface StreamCallback {
		public void onShutdown();
	}	
	
	private static Logger logger = LogManager.getLogger(GetStream.class);
	
	private static String msgToString(Object o) {
		try {
			return TwitterObjectFactory.getRawJSON(o).toString();
		} catch (Exception e) {
			if(o == null) {
				return "EXCEPTION [" + e.getMessage() +"]";
			} else {
				return o.toString();
			}
		}
	}
	
	private final Database database;
	private final WordPressUtil.OAuthSettings oauth;
	
	public GetStream(
			final Database database, 
			final WordPressUtil.OAuthSettings oauth) {
		this.database = database;
		this.oauth = oauth;
	}
	
	/**
	 * Open user stream for specific authenticated user and 
	 * start capturing event data to database.
	 * 
	 * @param wpUser
	 * @throws SQLException
	 * @throws IllegalStateException
	 * @throws TwitterException
	 */
	public TwitterStream createStream(
			final WordPressUtil.WpUser wpUser,
			final StreamCallback callback) 
	throws TwitterException {		
		
		final TwitterStream stream = new TwitterStreamFactory(
				getConfiguration(wpUser.oauthToken, wpUser.oauthTokenSecret)).getInstance();	
		
		long uid;
		long sleep = 10000; // 10 seconds
		while(true) {
			try {
				uid = stream.getId();
				break;
			} catch (TwitterException e) {
				logger.warn(e.getErrorMessage(), e);
				int code = e.getStatusCode();
				if(code > 499 && code < 600) {
					try{ Thread.sleep(sleep); }
					catch(InterruptedException ie) { }
					sleep *= 2;
					continue;
				}
				throw e;
			}
		}
		
		final long userID = uid;
		
		/*************************
		 * User Stream Listener
		 *************************/	
		UserStreamListener userListener = new UserStreamListener() {
			
			private void store(Event event, JSONObject json) {
				database.store(wpUser.id, userID, event, json.toString());
			}
			
			private void store(Event event, Object msg) {
				database.store(wpUser.id, userID, event, msgToString(msg));
			}
			
			private boolean userInArray(UserMentionEntity[] entities, long id) {
				for(UserMentionEntity entity : entities) {
					if(entity.getId() == id)
						return true;
				}
				return false;
			}
			
			/**  
			 * Capture status changes for authenticated user and write to data store.
			 *  @param status  
			 */
			public void onStatus(Status msg) {	
				if(userID == msg.getUser().getId()) {
					store(TWEET, msgToString(msg));
				} else if(msg.isRetweet()
							&& msg.getRetweetedStatus().getUser().getId() == userID) {
					// Necessary to stop retweets being logged as a general retweet and a friend retweet
					if(!friends.contains(new Long(msg.getUser().getId()))) {
						store(RETWEET, msgToString(msg));	
					}					
				} else if(msg.getUserMentionEntities().length > 0
						&& userInArray(msg.getUserMentionEntities(), userID)) {
					store(MENTION, msgToString(msg));
				}
			}

	        public void onDeletionNotice(StatusDeletionNotice msg) {
	        	/*
	        	 * djp - 26.01.16
	        	 * I think we'll ignore these notices for now, there
	        	 * seems to be an excessive number of these and I don't
	        	 * quite understand what they mean. They also aren't
	        	 * currently being used in the display application.
	        	 */
	        	/*ExceptionlessJSONObject json = new ExceptionlessJSONObject();
		        json.put("userId", msg.getUserId());
		        json.put("messageId", msg.getStatusId());
				store(STATUS_DELETION, json);*/
	        }

	        public void onDeletionNotice(long directMessageId, long userId) {
	        	ExceptionlessJSONObject json = new ExceptionlessJSONObject();
		        json.put("userId", userId);
		        json.put("messageId", directMessageId);
				store(MESSAGE_DELETION, json);
	        }
	        
	        /** 
	         * Captures when authenticated user favourites a tweet or when a tweet 
	         * is 'favourited' by  authenticated users follower.
	         */
	        public void onFavorite(User source, User target, Status msg) {	        	
				store(FAVOURITE, msg);        
	        }

	        public void onUnfavorite(User source, User target, Status msg) {
				store(UNFAVOURITE, msg);
	        }
	        
	        /** 
	         * Captures when authenticated user follows someone or is followed.
	         */
	        public void onFollow(User source, User followedUser) {     
	        	ExceptionlessJSONObject json = new ExceptionlessJSONObject();
		        json.put("sourceId", source.getId());
		        json.put("sourceName", source.getScreenName());
		        json.put("followedUserId", followedUser.getId());	
		        json.put("followedUserName", followedUser.getScreenName());
				store(FOLLOW, json);
	        }
	        
	        /** 
	         * Captures when authenticated user unfollows someone but does NOT capture
	         * when a follower unfollows authenticated user.
	         */
	        public void onUnfollow(User source, User unfollowedUser) {
				ExceptionlessJSONObject json = new ExceptionlessJSONObject();
	        	json.put("sourceId", source.getId());
	        	json.put("sourceName", source.getScreenName());
	        	json.put("unfollowedUserId", unfollowedUser.getId());	
	        	json.put("unfollowedUserName", unfollowedUser.getScreenName());
				store(UNFOLLOW, json);
	        }

	        /** 
	         * Captures when authenticated user send or receives a direct message.
	         * 
	         */
	        public void onDirectMessage(DirectMessage directMessage) {
	        	ExceptionlessJSONObject json = new ExceptionlessJSONObject();
	        	json.put("messageId", directMessage.getId());
	        	json.put("messageSenderId", directMessage.getSenderId());
	        	json.put("messageRecipientId", directMessage.getRecipientId());	
	        	json.put("messageText", directMessage.getText());
				store(MESSAGE, json);           
	        }

	        public void onBlock(User source, User blockedUser) {
	        	ExceptionlessJSONObject json = new ExceptionlessJSONObject();
	        	json.put("sourceId", source.getId());
	        	json.put("sourceName", source.getScreenName());
	        	json.put("blockedUserId", blockedUser.getId());	
	        	json.put("blockedUserName", blockedUser.getScreenName());
				store(BLOCK, json);
	        }

	        public void onUnblock(User source, User unblockedUser) {
				ExceptionlessJSONObject json = new ExceptionlessJSONObject();
	        	json.put("sourceId", source.getId());
	        	json.put("sourceName", source.getScreenName());
	        	json.put("unblockedUserId", unblockedUser.getId());	
	        	json.put("unblockedUserName", unblockedUser.getScreenName());
				store(UNBLOCK, json);
	        }

	        public void onRetweetedRetweet(User source, User target, Status msg) {
				store(RETWEETED_RETWEET, msgToString(msg));
	        }

	        public void onFavoritedRetweet(User source, User target, Status msg) {
				store(FAVOURITED_RETWEET, msgToString(msg));
	        }
	        
	        public void onQuotedTweet(User source, User target, Status msg) {
				store(QUOTED_TWEET, msgToString(msg)); 
	        }
	        
	        private final StatusListener statusListener = new StatusListener() {
                public void onStatus(Status msg) {
                	if(msg.isRetweet() && msg.getRetweetedStatus().getUser().getId() == userID) {	                		
    					if(friends.contains(new Long(msg.getUser().getId()))) {
    						store(FRIEND_RETWEET, msg);
    					} else {
    						store(FRIEND_OF_FRIEND_RETWEET, msg);
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
            
            private final HashSet<Long> friends = new HashSet<>();
	        
	        /** 
	         * Get the users friend list and invoke the Status listener to capture friends status changes
	         * @return 
	         * 
	         */
	        public void onFriendList(long[] ids) {
	        	friends.clear();
	        	for(long id : ids)
	        		friends.add(id);
	        	logger.debug("Friends list updated");
	            stream.replaceListener(statusListener, statusListener);
				FilterQuery query = new FilterQuery();
				query.follow(ids);
				stream.filter(query);
	        }
	        
	        public void onException(Exception ex) {
	        	logger.error(ex.getMessage(), ex);
	        	stream.clearListeners();
	        	stream.cleanUp();
	        	callback.onShutdown();
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
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onScrubGeo(long userId, long upToStatusId) {}
	        public void onStallWarning(StallWarning warning) {}		
		};

		stream.addListener(userListener);
		stream.user();	
		
		return stream;
	}
	
    /**
	 * Authorise Twitter OAuth credentials
	 * 
	 *  @param token
	 *  @param tokenSecret
	 *  @return
	 */
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
