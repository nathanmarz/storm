package org.apache.storm.solr.mapper;

import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Tuple;
import org.apache.solr.client.solrj.SolrRequest;

import java.io.Serializable;
import java.util.List;

/**
 * Created by hlouro on 7/22/15.
 */
public interface SolrMapper extends Serializable {
    String getCollection();
    SolrRequest toSolrRequest(ITuple tuple) throws SolrMapperException;
    SolrRequest toSolrRequest(List<? extends ITuple> tuples) throws SolrMapperException;
}
