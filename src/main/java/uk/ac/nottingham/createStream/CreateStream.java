package uk.ac.nottingham.createStream;

import java.sql.SQLException;

import twitter4j.TwitterException;

public class CreateStream {

	public static void main(String[] args) throws IllegalStateException, SQLException, TwitterException {

//		final String accessToken = args[0];
//		final String accessTokenSecret = args[1];		
		final String accessToken = "3216709625-Ts39k0LQsySucbf2Z4BuioZp6iQj1dYkQ8YYTOb";
		final String accessTokenSecret = "3Oz7RJWKkdrs1coqNjeXtmXuFyWC6jV0uLeCsNM8GOUSO";		
		GetStream stream = new GetStream();
		stream.createStream(accessToken, accessTokenSecret);
	}
}
