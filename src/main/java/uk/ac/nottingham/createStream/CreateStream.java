package uk.ac.nottingham.createStream;

import java.sql.SQLException;

import twitter4j.TwitterException;

public class CreateStream {

	public static void main(String[] args) throws IllegalStateException, SQLException, TwitterException {

//		final String accessToken = args[0];
//		final String accessTokenSecret = args[1];		
		final String accessToken = "";
		final String accessTokenSecret = "";		
		GetStream stream = new GetStream();
		stream.createStream(accessToken, accessTokenSecret);
	}
}
