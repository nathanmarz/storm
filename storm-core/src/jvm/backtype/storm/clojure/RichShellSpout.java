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
import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import java.util.Map;

public class RichShellSpout extends ShellSpout implements IRichSpout {
    private Map<String, StreamInfo> _outputs;

    public RichShellSpout(String[] command, Map<String, StreamInfo> outputs) {
        super(command);
        _outputs = outputs;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for(String stream: _outputs.keySet()) {
            StreamInfo def = _outputs.get(stream);
            if(def.is_direct()) {
                declarer.declareStream(stream, true, new Fields(def.get_output_fields()));
            } else {
                declarer.declareStream(stream, new Fields(def.get_output_fields()));
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
