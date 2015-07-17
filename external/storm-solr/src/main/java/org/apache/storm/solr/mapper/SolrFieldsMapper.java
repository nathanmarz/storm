package org.apache.storm.solr.mapper;

import static org.apache.storm.solr.schema.SolrFieldTypeFinder.FieldTypeWrapper;

import backtype.storm.tuple.ITuple;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.solr.schema.Field;
import org.apache.storm.solr.schema.Schema;
import org.apache.storm.solr.schema.builder.SchemaBuilder;
import org.apache.storm.solr.schema.SolrFieldTypeFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by hlouro on 7/24/15.
 */
public class SolrFieldsMapper implements SolrMapper {
    private static final Logger logger = LoggerFactory.getLogger(SolrFieldsMapper.class);
    private String collection;
    private SolrFieldTypeFinder typeFinder;
    private String multiValueFieldToken;

    public static class Builder {
        private String collection;
        private SolrFieldTypeFinder typeFinder;
        private String multiValueFieldToken = "|";

        public Builder(SchemaBuilder schemaBuilder) {
            setTypeFinder(schemaBuilder);
        }

        //TODO Handle the case where there may be no schema
        private void setTypeFinder(SchemaBuilder schemaBuilder) {
            Schema schema = schemaBuilder.getSchema();
            typeFinder = new SolrFieldTypeFinder(schema);
        }

        public Builder setCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder setDefaultCollection(SolrClient solrClient) {
            String defaultCollection = null;
            if (solrClient instanceof CloudSolrClient) {
                defaultCollection = ((CloudSolrClient) solrClient).getDefaultCollection();
            }
            this.collection = defaultCollection;
            return this;
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
        return request;
    }

    @Override
    public SolrRequest toSolrRequest(ITuple tuple) throws SolrMapperException {
        SolrInputDocument doc = buildDocument(tuple);
        UpdateRequest request = new UpdateRequest();
        request.add(doc);
        return request;
    }

    private SolrInputDocument buildDocument(ITuple tuple) {
        SolrInputDocument doc = new SolrInputDocument();

        for (String tupleField : tuple.getFields()) {
            FieldTypeWrapper fieldTypeWrapper = typeFinder.getFieldTypeWrapper(tupleField);
            if (fieldTypeWrapper != null) {
                Field field = fieldTypeWrapper.getField();
                if (field.isMultiValued()) {
                    addMultivalueFieldToDoc(doc, tupleField, tuple);
                } else {
                    addFieldToDoc(doc, tupleField, tuple);
                }
            } else {
                logger.info("Field [{}] does NOT match static or dynamic field declared in schema. Not added to document", tupleField);
            }
        }
        return doc;
    }

    private void addFieldToDoc(SolrInputDocument doc, String tupleField, ITuple tuple) {
        Object val = getValue(tupleField, tuple);
        logger.info("Adding to document (field, val) = ({}, {})", tupleField, val);
        doc.addField(tupleField, val);
    }

    private void addMultivalueFieldToDoc(SolrInputDocument doc, String tupleField, ITuple tuple) {
        String[] values = getValues(tupleField, tuple);
        for (String value : values) {
            logger.info("Adding {} to multivalue field document {}", value, tupleField);
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
}
