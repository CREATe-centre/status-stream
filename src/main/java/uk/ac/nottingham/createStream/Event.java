package uk.ac.nottingham.createStream;

/**
 * Enumerator for User and Status event types
 * 
 * @author pszjh
 */
public enum Event {
	STATUS,
	STATUS_DELETION,
	TWEET,
	QUOTED_TWEET,
	RETWEET,
	RETWEETED_RETWEET,
	FAVOURITED_RETWEET,
	FAVOURITE,
	UNFAVOURITE,
	FOLLOW,
	UNFOLLOW,
	BLOCK,
	UNBLOCK,
	MESSAGE,
	MESSAGE_DELETION,
	FRIEND_RETWEET,
	FRIEND_OF_FRIEND_RETWEET,
	MENTION
}
