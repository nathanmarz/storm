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
package org.apache.storm.serialization;

import org.apache.storm.Config;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.serialization.types.ArrayListSerializer;
import org.apache.storm.serialization.types.HashMapSerializer;
import org.apache.storm.serialization.types.HashSetSerializer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.trident.tuple.ConsList;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ListDelegate;
import org.apache.storm.utils.Utils;
import carbonite.JavaBridge;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.BigIntegerSerializer;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializationFactory {
    public static final Logger LOG = LoggerFactory.getLogger(SerializationFactory.class);

    public static Kryo getKryo(Map conf) {
        IKryoFactory kryoFactory = (IKryoFactory) Utils.newInstance((String) conf.get(Config.TOPOLOGY_KRYO_FACTORY));
        Kryo k = kryoFactory.getKryo(conf);
        k.register(byte[].class);

        /* tuple payload serializer is specified via configuration */
        String payloadSerializerName = (String)conf.get(Config.TOPOLOGY_TUPLE_SERIALIZER);
        try {
            Class serializerClass  = Class.forName(payloadSerializerName);
            Serializer serializer = resolveSerializerInstance(k, ListDelegate.class, serializerClass, conf);
            k.register(ListDelegate.class, serializer);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }

        k.register(ArrayList.class, new ArrayListSerializer());
        k.register(HashMap.class, new HashMapSerializer());
        k.register(HashSet.class, new HashSetSerializer());
        k.register(BigInteger.class, new BigIntegerSerializer());
        k.register(TransactionAttempt.class);
        k.register(Values.class);
        k.register(org.apache.storm.metric.api.IMetricsConsumer.DataPoint.class);
        k.register(org.apache.storm.metric.api.IMetricsConsumer.TaskInfo.class);
        k.register(ConsList.class);
        try {
            JavaBridge.registerPrimitives(k);
            JavaBridge.registerCollections(k);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        Map<String, String> registrations = normalizeKryoRegister(conf);

        kryoFactory.preRegister(k, conf);

        boolean skipMissing = (Boolean) conf.get(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS);
        for(Map.Entry<String, String> entry: registrations.entrySet()) {
            String serializerClassName = entry.getValue();
            try {
                Class klass = Class.forName(entry.getKey());
                Class serializerClass = null;
                if(serializerClassName!=null)
                    serializerClass = Class.forName(serializerClassName);
                if(serializerClass == null) {
                    k.register(klass);
                } else {
                    k.register(klass, resolveSerializerInstance(k, klass, serializerClass, conf));
                }
            } catch (ClassNotFoundException e) {
                if(skipMissing) {
                    LOG.info("Could not find serialization or class for " + serializerClassName + ". Skipping registration...");
                } else {
                    throw new RuntimeException(e);
                }
            }
        }

        kryoFactory.postRegister(k, conf);

        if (conf.get(Config.TOPOLOGY_KRYO_DECORATORS) != null) {
            for(String klassName : (List<String>)conf.get(Config.TOPOLOGY_KRYO_DECORATORS)) {
                try {
                    Class klass = Class.forName(klassName);
                    IKryoDecorator decorator = (IKryoDecorator)klass.newInstance();
                    decorator.decorate(k);
                } catch(ClassNotFoundException e) {
                    if(skipMissing) {
                        LOG.info("Could not find kryo decorator named " + klassName + ". Skipping registration...");
                    } else {
                        throw new RuntimeException(e);
                    }
                } catch(InstantiationException e) {
                    throw new RuntimeException(e);
                } catch(IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        kryoFactory.postDecorate(k, conf);

        return k;
    }

    public static class IdDictionary {
        Map<String, Map<String, Integer>> streamNametoId = new HashMap<>();
        Map<String, Map<Integer, String>> streamIdToName = new HashMap<>();

        /**
         * "{:a 1  :b 2} -> {1 :a  2 :b}"
         *
         * Note: Only one key wins if there are duplicate values.
         *       Which key wins is indeterminate:
         * "{:a 1  :b 1} -> {1 :a} *or* {1 :b}"
         */
        private static <K, V> Map<V, K> simpleReverseMap(Map<K, V> map) {
            Map<V, K> ret = new HashMap<V, K>();
            for (Map.Entry<K, V> entry : map.entrySet()) {
                ret.put(entry.getValue(), entry.getKey());
            }
            return ret;
        }

        public IdDictionary(StormTopology topology) {
            List<String> componentNames = new ArrayList<>(topology.get_spouts().keySet());
            componentNames.addAll(topology.get_bolts().keySet());
            componentNames.addAll(topology.get_state_spouts().keySet());

            for(String name: componentNames) {
                ComponentCommon common = Utils.getComponentCommon(topology, name);
                List<String> streams = new ArrayList<>(common.get_streams().keySet());
                streamNametoId.put(name, idify(streams));
                streamIdToName.put(name, simpleReverseMap(streamNametoId.get(name)));
            }
        }

        public int getStreamId(String component, String stream) {
            return streamNametoId.get(component).get(stream);
        }

        public String getStreamName(String component, int stream) {
            return streamIdToName.get(component).get(stream);
        }

        private static Map<String, Integer> idify(List<String> names) {
            Collections.sort(names);
            Map<String, Integer> ret = new HashMap<>();
            int i = 1;
            for(String name: names) {
                ret.put(name, i);
                i++;
            }
            return ret;
        }
    }

    private static Serializer resolveSerializerInstance(Kryo k, Class superClass, Class<? extends Serializer> serializerClass, Map conf) {
        try {
            try {
                return serializerClass.getConstructor(Kryo.class, Class.class, Map.class).newInstance(k, superClass, conf);
            } catch (Exception ex1) {
                try {
                    return serializerClass.getConstructor(Kryo.class, Class.class).newInstance(k, superClass);
                } catch (Exception ex2) {
                    try {
                        return serializerClass.getConstructor(Kryo.class, Map.class).newInstance(k, conf);
                    } catch (Exception ex3) {
                        try {
                            return serializerClass.getConstructor(Kryo.class).newInstance(k);
                        } catch (Exception ex4) {
                            try {
                                return serializerClass.getConstructor(Class.class, Map.class).newInstance(superClass, conf);
                            } catch (Exception ex5) {
                                try {
                                    return serializerClass.getConstructor(Class.class).newInstance(superClass);
                                } catch (Exception ex6) {
                                    return serializerClass.newInstance();
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("Unable to create serializer \""
                                               + serializerClass.getName()
                                               + "\" for class: "
                                               + superClass.getName(), ex);
        }
    }

    private static Map<String, String> normalizeKryoRegister(Map conf) {
        // TODO: de-duplicate this logic with the code in nimbus
        Object res = conf.get(Config.TOPOLOGY_KRYO_REGISTER);
        if(res==null) return new TreeMap<>();
        Map<String, String> ret = new HashMap<>();
        if(res instanceof Map) {
            ret = (Map<String, String>) res;
        } else {
            for(Object o: (List) res) {
                if(o instanceof Map) {
                    ret.putAll((Map) o);
                } else {
                    ret.put((String) o, null);
                }
            }
        }

        //ensure always same order for registrations with TreeMap
        return new TreeMap<>(ret);
    }
}
