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

package org.apache.storm.solr.schema.builder;

import com.google.gson.Gson;
import org.apache.storm.solr.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Scanner;

/**
 * Class that builds the {@link Schema} object from the JSON representation of the schema as returned by the
 * URL of the form http://localhost:8983/solr/gettingstarted/schema/ . This particular URL returns the schema
 * in JSON format for the gettingstarted example running locally.
 */
public class RestJsonSchemaBuilder implements SchemaBuilder {
    private static final Logger logger = LoggerFactory.getLogger(RestJsonSchemaBuilder.class);
    private Schema schema;


    /** Urls with the form http://localhost:8983/solr/gettingstarted/schema/ returns the schema in JSON format */
    public RestJsonSchemaBuilder(String solrHost, String solrPort, String collection) throws IOException {
        this(new URL("http://" + solrHost + ":" + solrPort + "/solr/" + collection + "/schema/"));
    }

    public RestJsonSchemaBuilder(String url) throws IOException {
        this(new URL(url));
    }

    public RestJsonSchemaBuilder(URL url) throws IOException {
        downloadSchema(url);
    }

    private void downloadSchema(URL url) throws IOException {
        String result;
        logger.debug("Downloading Solr schema info from: " + url);
        Scanner scanner = new Scanner(url.openStream());
        result = scanner.useDelimiter("\\Z").next();
        logger.debug("JSON Schema: " + result);

        Gson gson = new Gson();
        Schema.SchemaWrapper schemaWrapper = gson.fromJson(result, Schema.SchemaWrapper.class);
        this.schema = schemaWrapper.getSchema();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
