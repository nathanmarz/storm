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

package org.apache.storm.solr.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.apache.storm.solr.util.TestUtil;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class SolrFieldsSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    public static final List<Values> listValues = Lists.newArrayList(
            getValues("1"), getValues("2"), getValues("3"));

    private static Values getValues(String suf) {
        String suffix = "_fields_test_val_" + suf;
        return new Values(
                "id" + suffix,
                TestUtil.getDate(),
                "dc_title" + suffix,
                "Hugo%Miguel%Louro" + suffix,           // Multivalue field split by non default token %
                "dynamic_field" + suffix + "_txt",      // to match dynamic fields of the form "*_txt"
                "non_matching_field" + suffix);         // this field won't be indexed by solr
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        final Values values = listValues.get(rand.nextInt(listValues.size()));
        collector.emit(values);
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }

    public Fields getOutputFields() {
        return new Fields("id","date","dc_title","author","dynamic_field_txt","non_matching_field");
    }

    @Override
    public void close() {
        super.close();
    }
}
