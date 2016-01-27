package uk.ac.nottingham.create.stream.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.JSONException;

public class JSONObject 
extends twitter4j.JSONObject {
	
	private static Logger logger = LogManager.getLogger(JSONObject.class);
	
	public static JSONObject createFromSource(String source) {
		try {
			return new JSONObject(source);
		} catch (JSONException e) {
			logger.warn(e.getMessage(), e);
			return new JSONObject();
		}
	}
	
	public JSONObject() {
		super();
	}
	
	private JSONObject(String source) 
	throws JSONException {
		super(source);
	}
	
	@Override
	public JSONObject put(String key, long value) {
		try {
			return (JSONObject) super.put(key, value);
		} catch (JSONException e) {
			logger.error(e.getMessage(), e);
		}
		return this;
	}
	
	@Override
	public JSONObject put(String key, Object value) {
		try {
			return (JSONObject) super.put(key, value);
		} catch (JSONException e) {
			logger.error(e.getMessage(), e);
		}
		return this;
	}

}
