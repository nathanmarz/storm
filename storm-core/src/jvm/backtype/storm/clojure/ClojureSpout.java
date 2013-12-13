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
package backtype.storm.clojure;

import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import clojure.lang.IFn;
import clojure.lang.PersistentArrayMap;
import clojure.lang.Keyword;
import clojure.lang.Symbol;
import clojure.lang.RT;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClojureSpout implements IRichSpout {
    Map<String, StreamInfo> _fields;
    List<String> _fnSpec;
    List<String> _confSpec;
    List<Object> _params;
    
    ISpout _spout;
    
    public ClojureSpout(List fnSpec, List confSpec, List<Object> params, Map<String, StreamInfo> fields) {
        _fnSpec = fnSpec;
        _confSpec = confSpec;
        _params = params;
        _fields = fields;
    }
    

    @Override
    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
        try {
            IFn preparer = (IFn) hof.applyTo(RT.seq(_params));
            final Map<Keyword,Object> collectorMap = new PersistentArrayMap( new Object[] {
                Keyword.intern(Symbol.create("output-collector")), collector,
                Keyword.intern(Symbol.create("context")), context});
            List<Object> args = new ArrayList<Object>() {{
                add(conf);
                add(context);
                add(collectorMap);
            }};
            
            _spout = (ISpout) preparer.applyTo(RT.seq(args));
            //this is kind of unnecessary for clojure
            try {
                _spout.open(conf, context, collector);
            } catch(AbstractMethodError ame) {
                
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            _spout.close();
        } catch(AbstractMethodError ame) {
                
        }
    }

    @Override
    public void nextTuple() {
        try {
            _spout.nextTuple();
        } catch(AbstractMethodError ame) {
                
        }

    }

    @Override
    public void ack(Object msgId) {
        try {
            _spout.ack(msgId);
        } catch(AbstractMethodError ame) {
                
        }

    }

    @Override
    public void fail(Object msgId) {
        try {
            _spout.fail(msgId);
        } catch(AbstractMethodError ame) {
                
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(String stream: _fields.keySet()) {
            StreamInfo info = _fields.get(stream);
            declarer.declareStream(stream, info.is_direct(), new Fields(info.get_output_fields()));
        }
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        IFn hof = Utils.loadClojureFn(_confSpec.get(0), _confSpec.get(1));
        try {
            return (Map) hof.applyTo(RT.seq(_params));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void activate() {
        try {
            _spout.activate();
        } catch(AbstractMethodError ame) {
                
        }
    }

    @Override
    public void deactivate() {
        try {
            _spout.deactivate();
        } catch(AbstractMethodError ame) {
                
        }
    }
}
