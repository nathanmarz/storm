/*
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
package org.apache.storm.sql.kafka;

import org.apache.storm.spout.SchemeAsMultiScheme;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.storm.sql.runtime.*;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.kafka.trident.TridentKafkaState;
import org.apache.storm.kafka.trident.mapper.TridentTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.KafkaTopicSelector;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Create a Kafka spout/sink based on the URI and properties. The URI has the format of
 * kafka://zkhost:port/broker_path?topic=topic. The properties are in JSON format which specifies the producer config
 * of the Kafka broker.
 */
public class KafkaDataSourcesProvider implements DataSourcesProvider {
  private static final int DEFAULT_ZK_PORT = 2181;
  private static class StaticTopicSelector implements KafkaTopicSelector {
    private final String topic;

    private StaticTopicSelector(String topic) {
      this.topic = topic;
    }

    @Override
    public String getTopic(TridentTuple tuple) {
      return topic;
    }
  }

  private static class SqlKafkaMapper implements TridentTupleToKafkaMapper<Object, ByteBuffer> {
    private final int primaryKeyIndex;
    private final IOutputSerializer serializer;

    private SqlKafkaMapper(int primaryKeyIndex, IOutputSerializer serializer) {
      this.primaryKeyIndex = primaryKeyIndex;
      this.serializer = serializer;
    }

    @Override
    public Object getKeyFromTuple(TridentTuple tuple) {
      return tuple.get(primaryKeyIndex);
    }

    @Override
    public ByteBuffer getMessageFromTuple(TridentTuple tuple) {
      return serializer.write(tuple.getValues(), null);
    }
  }

  static class KafkaTridentSink extends BaseFunction {
    private transient TridentKafkaState state;
    private final String topic;
    private final int primaryKeyIndex;
    private final Properties producerProperties;
    private final List<String> fieldNames;

    private KafkaTridentSink(String topic, int primaryKeyIndex, Properties producerProperties,
                             List<String> fieldNames) {
      this.topic = topic;
      this.primaryKeyIndex = primaryKeyIndex;
      this.producerProperties = producerProperties;
      this.fieldNames = fieldNames;
    }

    @Override
    public void cleanup() {
      super.cleanup();
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
      JsonSerializer serializer = new JsonSerializer(fieldNames);
      SqlKafkaMapper m = new SqlKafkaMapper(primaryKeyIndex, serializer);
      state = new TridentKafkaState()
          .withKafkaTopicSelector(new StaticTopicSelector(topic))
          .withTridentTupleToKafkaMapper(m);
      state.prepare(producerProperties);
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      state.updateState(Collections.singletonList(tuple), collector);
    }
  }

  private static class KafkaTridentDataSource implements ISqlTridentDataSource {
    private final TridentKafkaConfig conf;
    private final String topic;
    private final int primaryKeyIndex;
    private final List<String> fields;
    private final Properties producerProperties;
    private KafkaTridentDataSource(TridentKafkaConfig conf, String topic, int primaryKeyIndex,
                                   Properties producerProperties, List<String> fields) {
      this.conf = conf;
      this.topic = topic;
      this.primaryKeyIndex = primaryKeyIndex;
      this.producerProperties = producerProperties;
      this.fields = fields;
    }

    @Override
    public ITridentDataSource getProducer() {
      return new OpaqueTridentKafkaSpout(conf);
    }

    @Override
    public Function getConsumer() {
      return new KafkaTridentSink(topic, primaryKeyIndex, producerProperties, fields);
    }
  }

  @Override
  public String scheme() {
    return "kafka";
  }

  @Override
  public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass,
                              List<FieldInfo> fields) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ISqlTridentDataSource constructTrident(URI uri, String inputFormatClass, String outputFormatClass,
                                                String properties, List<FieldInfo> fields) {
    int port = uri.getPort() != -1 ? uri.getPort() : DEFAULT_ZK_PORT;
    ZkHosts zk = new ZkHosts(uri.getHost() + ":" + port, uri.getPath());
    Map<String, String> values = parseURIParams(uri.getQuery());
    String topic = values.get("topic");
    Preconditions.checkNotNull(topic, "No topic of the spout is specified");
    TridentKafkaConfig conf = new TridentKafkaConfig(zk, topic);
    List<String> fieldNames = new ArrayList<>();
    int primaryIndex = -1;
    for (int i = 0; i < fields.size(); ++i) {
      FieldInfo f = fields.get(i);
      fieldNames.add(f.name());
      if (f.isPrimary()) {
        primaryIndex = i;
      }
    }
    Preconditions.checkState(primaryIndex != -1, "Kafka stream table must have a primary key");
    conf.scheme = new SchemeAsMultiScheme(new JsonScheme(fieldNames));
    ObjectMapper mapper = new ObjectMapper();
    Properties producerProp = new Properties();
    try {
      @SuppressWarnings("unchecked")
      HashMap<String, Object> map = mapper.readValue(properties, HashMap.class);
      @SuppressWarnings("unchecked")
      HashMap<String, Object> producerConfig = (HashMap<String, Object>) map.get("producer");
      Preconditions.checkNotNull(producerConfig, "Kafka Table must contain producer config");
      producerProp.putAll(producerConfig);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new KafkaTridentDataSource(conf, topic, primaryIndex, producerProp, fieldNames);
  }

  private static Map<String, String> parseURIParams(String query) {
    HashMap<String, String> res = new HashMap<>();
    if (query == null) {
      return res;
    }

    String[] params = query.split("&");
    for (String p : params) {
      String[] v = p.split("=", 2);
      if (v.length > 1) {
        res.put(v[0], v[1]);
      }
    }
    return res;
  }
}
