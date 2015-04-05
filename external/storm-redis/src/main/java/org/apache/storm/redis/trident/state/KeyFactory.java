package org.apache.storm.redis.trident.state;

import java.io.Serializable;
import java.util.List;

public interface KeyFactory extends Serializable {
	String build(List<Object> key);

	class DefaultKeyFactory implements KeyFactory {
		public String build(List<Object> key) {
			if (key.size() != 1)
				throw new RuntimeException("Default KeyFactory does not support compound keys");

			return (String) key.get(0);
		}
	}
}

