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

import backtype.storm.coordination.CoordinatedBolt.FinishedCallback;
import backtype.storm.generated.StreamInfo;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import clojure.lang.IFn;
import clojure.lang.PersistentArrayMap;
import clojure.lang.Keyword;
import clojure.lang.Symbol;
import clojure.lang.RT;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ClojureBolt implements IRichBolt, FinishedCallback {
    Map<String, StreamInfo> _fields;
    List<String> _fnSpec;
    List<String> _confSpec;
    List<Object> _params;
    
    IBolt _bolt;
    
    public ClojureBolt(List fnSpec, List confSpec, List<Object> params, Map<String, StreamInfo> fields) {
        _fnSpec = fnSpec;
        _confSpec = confSpec;
        _params = params;
        _fields = fields;
    }

    @Override
    public void prepare(final Map stormConf, final TopologyContext context, final OutputCollector collector) {
        IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
        try {
            IFn preparer = (IFn) hof.applyTo(RT.seq(_params));
            final Map<Keyword,Object> collectorMap = new PersistentArrayMap( new Object[] {
                Keyword.intern(Symbol.create("output-collector")), collector,
                Keyword.intern(Symbol.create("context")), context});
            List<Object> args = new ArrayList<Object>() {{
                add(stormConf);
                add(context);
                add(collectorMap);
            }};
            
            _bolt = (IBolt) preparer.applyTo(RT.seq(args));
            //this is kind of unnecessary for clojure
            try {
                _bolt.prepare(stormConf, context, collector);
            } catch(AbstractMethodError ame) {
                
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        _bolt.execute(input);
    }

    @Override
    public void cleanup() {
            try {
                _bolt.cleanup();
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
    public void finishedId(Object id) {
        if(_bolt instanceof FinishedCallback) {
            ((FinishedCallback) _bolt).finishedId(id);
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
}
