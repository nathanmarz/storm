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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.query.selector;

import org.apache.storm.tuple.ITuple;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.storm.cassandra.query.Column;

import java.io.Serializable;

public class FieldSelector implements Serializable {

    private String as;

    private String field;

    private boolean isNow;

    /**
     * Creates a new {@link FieldSelector} instance.
     * @param field the name of value.
     */
    public FieldSelector(String field) {
        this.field = field;
    }

    public Column select(ITuple t) {
        return new Column<>(as != null ? as : field, isNow ? UUIDs.timeBased() : t.getValueByField(field));
    }

    /**
     * Sets the fields as an automatically generated TimeUUID.
     * @return this.
     */
    public FieldSelector now() {
        this.isNow = true;
        return this;
    }

    /**
     * Sets an alias for this field.
     *
     * @param as the alias name.
     * @return this.
     */
    public FieldSelector as(String as) {
        this.as = as;
        return this;
    }
}
