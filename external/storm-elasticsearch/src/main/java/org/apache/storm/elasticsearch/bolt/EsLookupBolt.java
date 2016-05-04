/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.elasticsearch.bolt;

import java.util.Collection;

import org.apache.storm.elasticsearch.ElasticsearchGetRequest;
import org.apache.storm.elasticsearch.EsLookupResultOutput;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static org.elasticsearch.common.base.Preconditions.checkNotNull;

/**
 * @since 0.11
 */
public class EsLookupBolt extends AbstractEsBolt {

    private final ElasticsearchGetRequest getRequest;
    private final EsLookupResultOutput output;

    /**
     * @throws NullPointerException if any of the parameters is null
     */
    public EsLookupBolt(EsConfig esConfig, ElasticsearchGetRequest getRequest, EsLookupResultOutput output) {
        super(esConfig);
        checkNotNull(getRequest);
        checkNotNull(output);
        this.getRequest = getRequest;
        this.output = output;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Collection<Values> values = lookupValuesInEs(tuple);
            tryEmitAndAck(values, tuple);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(tuple);
        }
    }

    private Collection<Values> lookupValuesInEs(Tuple tuple) {
        GetRequest request = getRequest.extractFrom(tuple);
        GetResponse response = client.get(request).actionGet();
        return output.toValues(response);
    }

    private void tryEmitAndAck(Collection<Values> values, Tuple tuple) {
        for (Values value : values) {
            collector.emit(tuple, value);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(output.fields());
    }
}
