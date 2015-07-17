package org.apache.storm.solr.mapper;

import backtype.storm.tuple.ITuple;
import com.google.gson.Gson;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by hlouro on 7/24/15.
 */
public class SolrJsonMapper implements SolrMapper {
    private static final Logger logger = LoggerFactory.getLogger(SolrJsonMapper.class);
    private static final String JSON_UPDATE_URL = "/update/json/docs";
    private static final String CONTENT_TYPE = "application/json";;

    private final String jsonTupleField;
    private final String collection;

    public SolrJsonMapper(String collection, String jsonTupleField) {
        this.collection = collection;
        this.jsonTupleField = jsonTupleField;
    }

    /** Uses default collection */
    public SolrJsonMapper(SolrClient solrClient, String jsonTupleField) {
        String defaultCollection = null;
        if (solrClient instanceof CloudSolrClient) {
            defaultCollection = ((CloudSolrClient) solrClient).getDefaultCollection();
        }
        this.collection = defaultCollection;
        this.jsonTupleField = jsonTupleField;
    }

    @Override
    public String getCollection() {
        return collection;
    }

    @Override
    public SolrRequest toSolrRequest(List<? extends ITuple> tuples) throws SolrMapperException {
        final String jsonList = getJsonFromTuples(tuples);
        return createtSolrRequest(jsonList);
    }

    @Override
    public SolrRequest toSolrRequest(ITuple tuple) throws SolrMapperException {
        final String json = getJsonFromTuple(tuple);
        return createtSolrRequest(json);
    }

    private SolrRequest createtSolrRequest(String json) {
        ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(JSON_UPDATE_URL);
        ContentStream cs = new ContentStreamBase.StringStream(json, CONTENT_TYPE);
        request.addContentStream(cs);
        logger.info("Request generated with JSON: " + json);
        return request;
    }

    private String getJsonFromTuples(List<? extends ITuple> tuples) throws SolrMapperException {
        final StringBuilder jsonListBuilder = new StringBuilder("[");
        for (ITuple tuple : tuples) {
            final String json = getJsonFromTuple(tuple);
            jsonListBuilder.append(json).append(",");
        }
        jsonListBuilder.setCharAt(jsonListBuilder.length() - 1, ']');
        return jsonListBuilder.toString();
    }

    private String getJsonFromTuple(ITuple tuple) throws SolrMapperException {
        String json = "";
        if (tuple.contains(jsonTupleField)) {
            json = doGetJson(tuple.getValueByField(jsonTupleField));
        } else {
            throw new SolrMapperException("Tuple does not contain JSON object");
        }
        return json;
    }

    private String doGetJson(Object value) {
        String json = "";
        if (value instanceof String) {
            json = (String) value;          // Object associated with JSON field is already JSON
        } else {
            Gson gson = new Gson();
            json = gson.toJson(value);      // Serializes a Java object to JSON
        }
        return json;
    }
}
