package uk.ac.nottingham.create.stream;

import static uk.ac.nottingham.create.stream.Event.*;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.DirectMessage;
import twitter4j.Status;
import twitter4j.User;
import twitter4j.UserMentionEntity;
import twitter4j.UserStreamAdapter;
import uk.ac.nottingham.create.stream.util.Action;
import uk.ac.nottingham.create.stream.util.JSONObject;
import uk.ac.nottingham.create.stream.util.Util;
import uk.ac.nottingham.create.stream.util.WordPressUtil;

public class UserStreamListener
extends UserStreamAdapter {
	
	private static final Logger logger = LogManager.getLogger(UserStreamListener.class);  
	
	private final WordPressUtil.WpUser wpUser;
	
	private final long twitterID;
	
	private final Set<Long> friends;
	
	private final Action<long[]> onFriendList;
	
	private final Database database;
	
	public UserStreamListener(
			WordPressUtil.WpUser wpUser,
			long twitterID,
			Set<Long> friends,
			Action<long[]> onFriendList,
			Database database) {
		this.wpUser = wpUser;
		this.twitterID = twitterID;
		this.friends = friends;
		this.onFriendList = onFriendList;
		this.database = database;
	}

	public void onStatus(Status msg) {	
		if(twitterID == msg.getUser().getId()) {
			store(TWEET, msg);
		} else if(msg.isRetweet()
					&& msg.getRetweetedStatus().getUser().getId() == twitterID
					&& !friends.contains(msg.getUser().getId())) {
				store(RETWEET, msg);	
		} else if(!msg.isRetweet() 
				&& isUserMentioned(msg.getUserMentionEntities(), twitterID)) {
			store(MENTION, msg);
		}
	}

    public void onDeletionNotice(long directMessageId, long userId) {
    	JSONObject json = new JSONObject()
    			.put("userId", userId)
    			.put("messageId", directMessageId);
		store(MESSAGE_DELETION, json);
    }
    
    public void onFavorite(User source, User target, Status msg) {	    
    	JSONObject json = new JSONObject()
    			.put("sourceId", source.getId())
    			.put("sourceName", source.getScreenName())
    			.put("targetId", target.getId())
    			.put("targetUserName", target.getScreenName())
    			.put("status", JSONObject.createFromSource(Util.toString(msg)));
    	if(source.getId() == twitterID) {
    		store(YOU_FAVOURITED, json);
    	} else {
    		store(FAVOURITED_YOU, json);
    	}		
    }

    public void onUnfavorite(User source, User target, Status msg) {
    	JSONObject json = new JSONObject()
    			.put("sourceId", source.getId())
    			.put("sourceName", source.getScreenName())
    			.put("targetId", target.getId())
    			.put("targetUserName", target.getScreenName())
    			.put("status", JSONObject.createFromSource(Util.toString(msg)));
    	if(source.getId() == twitterID) {
    		store(YOU_UNFAVOURITED, json);
    	} else {
    		store(UNFAVOURITED_YOU, json);
    	}
    }
    
    public void onFollow(User source, User followedUser) {
    	JSONObject json = new JSONObject()
    			.put("sourceId", source.getId())
    			.put("sourceName", source.getScreenName())
    			.put("followedUserId", followedUser.getId())
    			.put("followedUserName", followedUser.getScreenName());
    	if(source.getId() == twitterID) {
    		store(YOU_FOLLOWED, json);
    		friends.add(followedUser.getId());
    		onFriendList(Util.toArray(friends));
    	} else {
    		store(FOLLOWED_YOU, json);
    	}
    }
    
    public void onUnfollow(User source, User unfollowedUser) {
    	// TODO - When a user unfollows 'you', the streaming API
    	// doesn't send that, is there a workaround?
		JSONObject json = new JSONObject()
				.put("sourceId", source.getId())
				.put("sourceName", source.getScreenName())
				.put("unfollowedUserId", unfollowedUser.getId())
				.put("unfollowedUserName", unfollowedUser.getScreenName());
		if(source.getId() == twitterID) {
			store(YOU_UNFOLLOWED, json);
			friends.remove(unfollowedUser.getId());
			onFriendList(Util.toArray(friends));
		} else {
			store(UNFOLLOWED_YOU, json);
		}
    }

    public void onDirectMessage(DirectMessage directMessage) {
    	JSONObject json = new JSONObject()
    			.put("messageId", directMessage.getId())
    			.put("messageSenderId", directMessage.getSenderId())
    			.put("messageRecipientId", directMessage.getRecipientId())
    			.put("messageText", directMessage.getText());
		store(MESSAGE, json);           
    }

    public void onBlock(User source, User blockedUser) {
    	JSONObject json = new JSONObject()
    			.put("sourceId", source.getId())
    			.put("sourceName", source.getScreenName())
    			.put("blockedUserId", blockedUser.getId())
    			.put("blockedUserName", blockedUser.getScreenName());
		store(BLOCK, json);
    }

    public void onUnblock(User source, User unblockedUser) {
		JSONObject json = new JSONObject()
				.put("sourceId", source.getId())
				.put("sourceName", source.getScreenName())
				.put("unblockedUserId", unblockedUser.getId())
				.put("unblockedUserName", unblockedUser.getScreenName());
		store(UNBLOCK, json);
    }

    public void onRetweetedRetweet(User source, User target, Status msg) {
		store(RETWEETED_RETWEET, msg);
    }

    public void onFavoritedRetweet(User source, User target, Status msg) {
		store(FAVOURITED_RETWEET, msg);
    }
    
    public void onQuotedTweet(User source, User target, Status msg) {
		store(QUOTED_TWEET, msg); 
    }
    
    public void onFriendList(long[] ids) {
    	logger.debug("Friend list modified");
    	friends.clear();
    	for(long id : ids) {
    		friends.add(id);
    	}
		onFriendList.doAction(ids);
    }
    
    private void store(Event event, JSONObject json) {
		database.store(wpUser.id, twitterID, event, json.toString());
	}
	
	private void store(Event event, Object msg) {
		database.store(wpUser.id, twitterID, event, Util.toString(msg));
	}
	
	private boolean isUserMentioned(UserMentionEntity[] entities, long id) {
		if(entities == null || entities.length == 0) {
			return false;
		}			
		for(UserMentionEntity entity : entities) {
			if(entity.getId() == id) {
				return true;
			}
		}
		return false;
	}
    
}
