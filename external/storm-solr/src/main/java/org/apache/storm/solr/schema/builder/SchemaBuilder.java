package org.apache.storm.solr.schema.builder;

import org.apache.storm.solr.schema.Schema;

import java.io.Serializable;

/**
 * Created by hlouro on 7/28/15.
 */
public interface SchemaBuilder extends Serializable {
    Schema getSchema();
}
