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
package org.apache.storm.flux.test;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestBolt extends BaseBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TestBolt.class);

    private String foo;
    private String bar;
    private String fooBar;

    public static enum TestEnum {
        FOO,
        BAR
    }

    public TestBolt(TestEnum te){

    }

    public TestBolt(TestEnum te, float f){

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        LOG.info("{}", tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    // config methods
    public void withFoo(String foo){
        this.foo = foo;
    }
    public void withBar(String bar){
        this.bar = bar;
    }

    public void withFooBar(String foo, String bar){
        this.fooBar = foo + bar;
    }

    public String getFoo(){
        return this.foo;
    }
    public String getBar(){
        return this.bar;
    }

    public String getFooBar(){
        return this.fooBar;
    }
}
