package org.apache.storm.solr.schema;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Class representing the SolrSchema as returned by the REST call to the URL of the form
 * http://localhost:8983/solr/gettingstarted/schema. This particular URL returns the schema in JSON format for the
 * <a href="http://lucene.apache.org/solr/quickstart.html">Solr quickstart</a> example running locally.
 * <p></p>
 * Created by hlouro on 7/27/15.
 */
public class Schema implements Serializable {
    private String name;
    private String version;
    private String uniqueKey;
    private List<FieldType> fieldTypes;
    private List<Field> fields;
    private List<Field> dynamicFields;
    private List<CopyField> copyFields;

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public List<FieldType> getFieldTypes() {
        return Collections.unmodifiableList(fieldTypes);
    }

    public List<Field> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public List<Field> getDynamicFields() {
        return Collections.unmodifiableList(dynamicFields);
    }

    public List<CopyField> getCopyFields() {
        return Collections.unmodifiableList(copyFields);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public void setFieldTypes(List<FieldType> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public void setDynamicFields(List<Field> dynamicFields) {
        this.dynamicFields = dynamicFields;
    }

    public void setCopyFields(List<CopyField> copyFields) {
        this.copyFields = copyFields;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "name='" + name + '\'' +
                ", version='" + version + '\'' +
                ", uniqueKey='" + uniqueKey + '\'' +
                ", fieldTypes=" + fieldTypes +
                ", fields=" + fields +
                ", dynamicFields=" + dynamicFields +
                ", copyFields=" + copyFields +
                '}';
    }
    
    public static class SchemaWrapper implements Serializable {
        Schema schema;

        public Schema getSchema() {
            return schema;
        }
    }
}
