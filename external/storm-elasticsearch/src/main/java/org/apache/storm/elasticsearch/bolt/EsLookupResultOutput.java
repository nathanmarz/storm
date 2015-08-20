package org.apache.storm.elasticsearch.bolt;

import java.util.Collection;

import org.elasticsearch.action.get.GetResponse;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public interface EsLookupResultOutput {

    Collection<Values> toValues(GetResponse response);

    Fields fields();
}
