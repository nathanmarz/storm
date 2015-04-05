package org.apache.storm.redis.trident.state;

import storm.trident.state.Serializer;

import java.io.Serializable;

public class Options<T> implements Serializable {
	public int localCacheSize = 1000;
	public String globalKey = "$REDIS-MAP-STATE-GLOBAL";
	KeyFactory keyFactory = null;
	public Serializer<T> serializer = null;
	public String hkey = null;
}
