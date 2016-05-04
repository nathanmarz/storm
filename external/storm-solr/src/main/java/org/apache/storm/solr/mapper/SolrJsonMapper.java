/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.solr.mapper;

import org.apache.storm.tuple.ITuple;
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

public class SolrJsonMapper implements SolrMapper {
    private static final Logger logger = LoggerFactory.getLogger(SolrJsonMapper.class);
    private static final String CONTENT_TYPE = "application/json";

    private final String jsonUpdateUrl;
    private final String jsonTupleField;
    private final String collection;

    public static class Builder {
        private String jsonUpdateUrl = "/update/json/docs";
        private final String jsonTupleField;
        private final String collection;

        /**
         * {@link SolrJsonMapper} builder class.
         * @param collection Name of the Solr collection to update using the SolrRequest
         * @param jsonTupleField Name of the tuple field that contains the JSON object used to update the Solr index
         */
        public Builder(String collection, String jsonTupleField) {
            this.collection = collection;
            this.jsonTupleField = jsonTupleField;
        }

        /**
         * {@link SolrJsonMapper} builder class.
         * @param solrClient {@link SolrClient} implementation from where to extract the default Solr collection, if any defined.
         * @param jsonTupleField Name of the tuple field that contains the JSON object used to update the Solr index
         */
        public Builder(SolrClient solrClient, String jsonTupleField) {
            String defaultCollection = null;
            if (solrClient instanceof CloudSolrClient) {
                defaultCollection = ((CloudSolrClient) solrClient).getDefaultCollection();
            }
            this.collection = defaultCollection;
            this.jsonTupleField = jsonTupleField;
        }

        /**
         * Sets the URL endpoint where to send {@link SolrRequest} requests in JSON format. This URL is defined
         * by Solr and for the most part you will not have to call this method to to override this value.
         * Currently the default value is /update/json/docs.
         * This method is available to support future evolutions of the Solr API.
         *
         * @param jsonUpdateUrl URL endpoint to where send {@link SolrRequest}
         * @return {@link Builder object} used to build instances of {@link SolrJsonMapper}
         */
        public Builder setJsonUpdateUrl(String jsonUpdateUrl) {
            this.jsonUpdateUrl = jsonUpdateUrl;
            return this;
        }

        public SolrJsonMapper build() {
            return new SolrJsonMapper(this);
        }
    }

    private SolrJsonMapper(Builder builder) {
        jsonTupleField = builder.jsonTupleField ;
        collection = builder.collection;
        jsonUpdateUrl = builder.jsonUpdateUrl;
    }

    @Override
    public String getCollection() {
        return collection;
    }

    /**
     *
     * @param tuples
     * @return
     * @throws SolrMapperException if the tuple does not contain the
     */
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
        final ContentStreamUpdateRequest request = new ContentStreamUpdateRequest(jsonUpdateUrl);
        final ContentStream cs = new ContentStreamBase.StringStream(json, CONTENT_TYPE);
        request.addContentStream(cs);
        if (logger.isDebugEnabled()) {
            logger.debug("Request generated with JSON: " + json);
        }
        return request;
    }

    // Builds a JSON list
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
