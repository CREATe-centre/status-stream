package uk.ac.nottingham.create.stream;

import static uk.ac.nottingham.create.stream.Event.*;

import java.util.Set;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import uk.ac.nottingham.create.stream.util.Util;
import uk.ac.nottingham.create.stream.util.WordPressUtil;

public class FriendStreamListener
extends StatusAdapter {

private final WordPressUtil.WpUser wpUser;
	
	private final long twitterID;
	
	private final Set<Long> friends;
	
	private final Database database;
	
	public FriendStreamListener(
			WordPressUtil.WpUser wpUser,
			long twitterID,
			Set<Long> friends,
			Database database) {
		this.wpUser = wpUser;
		this.twitterID = twitterID;
		this.friends = friends;
		this.database = database;
	}
	
	@Override
    public void onStatus(Status msg) {
    	if(msg.isRetweet() 
    			&& msg.getRetweetedStatus().getUser().getId() == twitterID) {	                		
			if(friends.contains(new Long(msg.getUser().getId()))) {
				store(FRIEND_RETWEET, msg);
			} else {
				store(FRIEND_OF_FRIEND_RETWEET, msg);
			}
    	}	                	
    }
	
	private void store(Event event, Object msg) {
		database.store(wpUser.id, twitterID, event, Util.toString(msg));
	}

}
