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

package org.apache.storm.solr.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Class containing all the information relating fields with their types. This information is wrapped in the class
 * {@link FieldTypeWrapper}
 */
public class SolrFieldTypeFinder implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SolrFieldTypeFinder.class);
    private Schema schema;
    private Map<String, FieldTypeWrapper> fieldToWrapper;

    /**
     * Class wrapping all the information for fields and types
     * */
    public static class FieldTypeWrapper implements Serializable {
        Field field;
        FieldType type;

        public FieldTypeWrapper(Field field, FieldType type) {
            this.field = field;
            this.type = type;
        }

        public Field getField() {
            return field;
        }

        public FieldType getType() {
            return type;
        }

        @Override
        public String toString() {
            return "FieldTypeWrapper{" +
                    "field=" + field +
                    ", type=" + type +
                    '}';
        }
    }

    /**
     * Initiates class containing all the information relating fields with their types.
     * This information is parsed from the schema
     * @param schema SolrSchema containing the information about fields and types
     * */
    public SolrFieldTypeFinder(Schema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema object is null");
        }
        this.schema = schema;
        this.fieldToWrapper = new HashMap<>();
        buildMap();
    }

    private void buildMap() {
        final List<FieldType> fieldTypes = schema.getFieldTypes();
        // static fields
        buildMapForFields(fieldTypes, schema.getFields());
        // dynamic fields
        buildMapForFields(fieldTypes, schema.getDynamicFields());
        if (log.isDebugEnabled()) {
            log.debug("Completed building FieldType Map: " + fieldToWrapper);
        }
    }

    private void buildMapForFields(List<FieldType> fieldTypes, List<Field> fields) {
        for (Field field: fields) {
            String type = field.getType();
            int idx = indexOf(fieldTypes, type);    // idx - index of the type of this field in the FieldType list
            if (idx != -1) {
              fieldToWrapper.put(field.getName(), new FieldTypeWrapper(field, fieldTypes.get(idx)));
            }
        }
    }

    private int indexOf(List<FieldType> fieldTypes, String type) {
        int i = 0;
        for (FieldType fieldType : fieldTypes) {
            if (fieldType.getName().equals(type)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    /**
     * Finds the schema defined field that matches the input parameter, if any. It can be a dynamic field, in
     * which case it will return the pattern of the dynamic field that matches the input parameter.
     * @param fieldName The name of the field to get info for
     * @return The {@link FieldTypeWrapper} that matches the input parameter, or null if none found
     * */
    public FieldTypeWrapper getFieldTypeWrapper(String fieldName) {
        FieldTypeWrapper typeWrapper = fieldToWrapper.get(fieldName);
            // => field name does not match static field, test if it matches dynamic field
        if (typeWrapper == null) {
            for (String pattern : fieldToWrapper.keySet()) {
                if (matchesDynamicField(fieldName, pattern)) {
                    typeWrapper = fieldToWrapper.get(pattern);
                }
            }
        }
        log.debug("Solr Field Name = {}, Solr Type = {}", fieldName, typeWrapper);
        return typeWrapper;
    }

    public Set<String> getAllSolrFieldTypes() {
        Collection<FieldTypeWrapper> typeWrappers = fieldToWrapper.values();
        Set<String> fieldTypeClasses = new TreeSet<>();
        for (FieldTypeWrapper typeWrapper : typeWrappers) {
            fieldTypeClasses.add(typeWrapper.getType().getClazz());
        }
        log.debug("Field type classes present in schema: {}", fieldTypeClasses);
        return fieldTypeClasses;
    }

    public boolean matchesField(String fieldName) {
        return fieldToWrapper.containsKey(fieldName);
    }

    public boolean matchesDynamicField(String fieldName) {
        for (String pattern : fieldToWrapper.keySet()) {
            if (matchesDynamicField(fieldName, pattern)) {
                return true;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Field [{}] did NOT match any dynamic field present in {}", fieldName, fieldToWrapper.keySet());
        }
        return false;
    }

    public boolean matchesDynamicField(String fieldName, String pattern) {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern and fieldName arguments cannot be null");
        }
        if (pattern.startsWith("*")) {
            if (fieldName.endsWith(pattern.substring(1))) {
                log.debug("Field [{}] MATCHES dynamic field {}", fieldName, pattern);
                return true;
            }
        } else if (pattern.endsWith("*")) {
            if (fieldName.startsWith(pattern.substring(0, pattern.length()-1))) {
                log.debug("Field [{}] MATCHES dynamic field {}", fieldName, pattern);
                return true;
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Field [{}] did NOT match any dynamic field present in {}", fieldName, fieldToWrapper.keySet());
        }
        return false;
    }
}
