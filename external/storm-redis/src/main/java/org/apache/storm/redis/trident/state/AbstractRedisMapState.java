package org.apache.storm.redis.trident.state;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import storm.trident.state.*;
import storm.trident.state.map.IBackingMap;

import java.util.*;

public abstract class AbstractRedisMapState<T> implements IBackingMap<T> {
	public static final EnumMap<StateType, Serializer> DEFAULT_SERIALIZERS = Maps.newEnumMap(ImmutableMap.of(
			StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer(),
			StateType.TRANSACTIONAL, new JSONTransactionalSerializer(),
			StateType.OPAQUE, new JSONOpaqueSerializer()
	));

	@Override public List<T> multiGet(List<List<Object>> keys) {
		if (keys.size() == 0) {
			return Collections.emptyList();
		}

		List<String> stringKeys = buildKeys(keys);
		List<String> values = retrieveValuesFromRedis(stringKeys);

		return deserializeValues(keys, values);
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		if (keys.size() == 0) {
			return;
		}

		Map<String, String> keyValues = new HashMap<String, String>();
		for (int i = 0; i < keys.size(); i++) {
			String val = new String(getSerializer().serialize(vals.get(i)));
			String redisKey = getKeyFactory().build(keys.get(i));
			keyValues.put(redisKey, val);
		}

		updateStatesToRedis(keyValues);
	}

	private List<String> buildKeys(List<List<Object>> keys) {
		List<String> stringKeys = new ArrayList<String>();

		for (List<Object> key : keys) {
			stringKeys.add(getKeyFactory().build(key));
		}

		return stringKeys;
	}

	private List<T> deserializeValues(List<List<Object>> keys, List<String> values) {
		List<T> result = new ArrayList<T>(keys.size());
		for (String value : values) {
			if (value != null) {
				result.add((T) getSerializer().deserialize(value.getBytes()));
			} else {
				result.add(null);
			}
		}
		return result;
	}

	protected abstract Serializer getSerializer();
	protected abstract KeyFactory getKeyFactory();
	protected abstract List<String> retrieveValuesFromRedis(List<String> keys);
	protected abstract void updateStatesToRedis(Map<String, String> keyValues);
}
