package org.apache.storm.cassandra.query;

import backtype.storm.tuple.ITuple;

import java.io.Serializable;
import java.util.Map;

/**
 * This interface may be used to retrieve a cassandra bound query either from storm config
 * or the tuple being proceed.
 *
 */
public interface ContextQuery extends Serializable {

    /**
     * Resolves a cassandra bound query.
     *
     * @param config the storm configuration
     * @param tuple the tuple being proceed.
     *
     * @return a string bound query.
     */
    public String resolves(Map config, ITuple tuple);

    /**
     * Static implementation of {@link ContextQuery} interface.
     */
    public static final class StaticContextQuery implements ContextQuery {
        private final String value;

        /**
         * Creates a new {@link StaticContextQuery} instance.
         * @param value
         */
        public StaticContextQuery(String value) {
            this.value = value;
        }

        @Override
        public String resolves(Map config, ITuple tuple) {
            return value;
        }
    }

    /**
     * Default {@link BoundQueryContext} implementation to retrieve a bound query
     * identified by the provided key.
     */
    public static final class BoundQueryContext implements ContextQuery {
        private String key;

        public BoundQueryContext(String key) {
            this.key = key;
        }

        @Override
        public String resolves(Map config, ITuple tuple) {
            if (config.containsKey(key)) return (String) config.get(key);

            throw new IllegalArgumentException("Bound query '" + key + "' does not exist in configuration");
        }
    }

    /**
     * Default {@link BoundQueryNamedByFieldContext} implementation to retrieve a bound query named by
     * the value of a specified tuple field.
     */
    public static final class BoundQueryNamedByFieldContext implements ContextQuery {

        private String fieldName;

        public BoundQueryNamedByFieldContext(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String resolves(Map config, ITuple tuple) {
            String name = tuple.getStringByField(fieldName);
            if (config.containsKey(name)) return (String) config.get(name);
            throw new IllegalArgumentException("Bound query '" + name + "' does not exist in configuration");
        }
    }
}
