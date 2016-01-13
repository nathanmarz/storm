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

import static org.apache.storm.solr.schema.SolrFieldTypeFinder.FieldTypeWrapper;

import org.apache.storm.tuple.ITuple;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.solr.schema.FieldType;
import org.apache.storm.solr.schema.Schema;
import org.apache.storm.solr.schema.builder.SchemaBuilder;
import org.apache.storm.solr.schema.SolrFieldTypeFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class SolrFieldsMapper implements SolrMapper {
    private static final Logger log = LoggerFactory.getLogger(SolrFieldsMapper.class);
    private String collection;
    private SolrFieldTypeFinder typeFinder;
    private String multiValueFieldToken;

    public static class Builder {
        private String collection;
        private SolrFieldTypeFinder typeFinder;
        private String multiValueFieldToken = "|";

        /**
         * {@link SolrFieldsMapper} builder class.
         * @param schemaBuilder Solr {@link Schema} builder class
         * @param collection Name of the Solr collection to update using the SolrRequest
         */
        public Builder(SchemaBuilder schemaBuilder, String collection) {
            setTypeFinder(schemaBuilder);
            this.collection = collection;
        }

        /**
         * {@link SolrFieldsMapper} builder class.
         * @param schemaBuilder Solr {@link Schema} builder class
         * @param solrClient {@link SolrClient} implementation from where to extract the default Solr collection, if any defined.
         */
        public Builder(SchemaBuilder schemaBuilder, SolrClient solrClient) {
            setTypeFinder(schemaBuilder);
        }

        //TODO Handle the case where there may be no schema
        private void setTypeFinder(SchemaBuilder schemaBuilder) {
            Schema schema = schemaBuilder.getSchema();
            typeFinder = new SolrFieldTypeFinder(schema);
        }

         // Sets {@link SolrFieldsMapper} to use the default Solr collection if there is one defined
        private void setDefaultCollection(SolrClient solrClient) {
            String defaultCollection = null;
            if (solrClient instanceof CloudSolrClient) {
                defaultCollection = ((CloudSolrClient) solrClient).getDefaultCollection();
            }
            this.collection = defaultCollection;
        }

        /**
         * Sets the token that separates multivalue fields in tuples. The default token is |
         * */
        public Builder setMultiValueFieldToken(String multiValueFieldToken) {
            this.multiValueFieldToken = multiValueFieldToken;
            return this;
        }

        public SolrFieldsMapper build() {
            return new SolrFieldsMapper(this);
        }
    }

    private SolrFieldsMapper(Builder builder) {
        this.collection = builder.collection;
        this.typeFinder = builder.typeFinder;
        this.multiValueFieldToken = builder.multiValueFieldToken;
        log.debug("Created {} with the following configuration: [{}] "
                + this.getClass().getSimpleName(), this.toString());
    }

    @Override
    public String getCollection() {
        return collection;
    }

    @Override
    public SolrRequest toSolrRequest(List<? extends ITuple> tuples) throws SolrMapperException {
        List<SolrInputDocument> docs = new LinkedList<>();
        for (ITuple tuple : tuples) {
            docs.add(buildDocument(tuple));
        }
        UpdateRequest request = new UpdateRequest();
        request.add(docs);
        log.debug("Created SolrRequest with content: {}", docs);
        return request;
    }

    @Override
    public SolrRequest toSolrRequest(ITuple tuple) throws SolrMapperException {
        SolrInputDocument doc = buildDocument(tuple);
        UpdateRequest request = new UpdateRequest();
        request.add(doc);
        log.debug("Created SolrRequest with content: {}", doc);
        return request;
    }

    private SolrInputDocument buildDocument(ITuple tuple) {
        SolrInputDocument doc = new SolrInputDocument();

        for (String tupleField : tuple.getFields()) {
            FieldTypeWrapper fieldTypeWrapper = typeFinder.getFieldTypeWrapper(tupleField);
            if (fieldTypeWrapper != null) {
                FieldType fieldType = fieldTypeWrapper.getType();
                if (fieldType.isMultiValued()) {
                    addMultivalueFieldToDoc(doc, tupleField, tuple);
                } else {
                    addFieldToDoc(doc, tupleField, tuple);
                }
            } else {
                log.debug("Field [{}] does NOT match static or dynamic field declared in schema. Not added to document", tupleField);
            }
        }
        return doc;
    }

    private void addFieldToDoc(SolrInputDocument doc, String tupleField, ITuple tuple) {
        Object val = getValue(tupleField, tuple);
        log.debug("Adding to document (field, val) = ({}, {})", tupleField, val);
        doc.addField(tupleField, val);
    }

    private void addMultivalueFieldToDoc(SolrInputDocument doc, String tupleField, ITuple tuple) {
        String[] values = getValues(tupleField, tuple);
        for (String value : values) {
            log.debug("Adding {} to multivalue field document {}", value, tupleField);
            doc.addField(tupleField, value);
        }
    }

    private Object getValue(String field, ITuple tuple) {
        return tuple.getValueByField(field);
    }

    private String[] getValues(String field, ITuple tuple) {
        String multiValueField = tuple.getStringByField(field);
        String[] values = multiValueField.split(multiValueFieldToken);
        return values;
    }

    @Override
    public String toString() {
        return "SolrFieldsMapper{" +
                "collection='" + collection + '\'' +
                ", typeFinder=" + typeFinder +
                ", multiValueFieldToken='" + multiValueFieldToken + '\'' +
                '}';
    }
}
