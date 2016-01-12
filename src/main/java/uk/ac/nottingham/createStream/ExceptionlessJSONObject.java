package uk.ac.nottingham.createStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.JSONException;
import twitter4j.JSONObject;

public class ExceptionlessJSONObject 
extends JSONObject {

	private static Logger logger = LogManager.getLogger(ExceptionlessJSONObject.class);
	
	@Override
	public JSONObject put(String key, long value) {
		try {
			return super.put(key, value);
		} catch (JSONException e) {
			logger.error(e.getMessage(), e);
		}
		return this;
	}
	
	@Override
	public JSONObject put(String key, Object value) {
		try {
			return super.put(key, value);
		} catch (JSONException e) {
			logger.error(e.getMessage(), e);
		}
		return this;
	}

}
