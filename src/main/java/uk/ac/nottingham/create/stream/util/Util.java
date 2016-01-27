package uk.ac.nottingham.create.stream.util;

import java.util.Set;

import twitter4j.TwitterObjectFactory;

public class Util {

	public static String toString(Object o) {
		try {
			return TwitterObjectFactory.getRawJSON(o).toString();
		} catch (Exception e) {
			if(o == null) {
				return "null";
			} else {
				return o.toString();
			}
		}
	}
	
	public static long[] toArray(Set<Long> s) {
		Long[] a = s.toArray(new Long[0]);
		long[] b = new long[a.length];
		for(int i = 0; i < a.length; i++) {
			b[i] = a[i].longValue();
		}
		return b;
	}

}
