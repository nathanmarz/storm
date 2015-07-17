package org.apache.storm.solr.schema.builder;

import com.google.gson.Gson;
import org.apache.storm.solr.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Scanner;

/**
 * Class that buils the {@link Schema} object from the JSON representation of the schema as returned by the
 * URL of the form http://localhost:8983/solr/gettingstarted/schema/ . This particular URL returns the schema
 * in JSON format for the gettingstarted example running locally.
 * <p></p>
 * Created by hlouro on 7/28/15.
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
