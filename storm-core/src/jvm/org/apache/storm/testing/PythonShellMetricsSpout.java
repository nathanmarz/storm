/*
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
package org.apache.storm.testing;

import java.util.Map;

import org.apache.storm.metric.api.rpc.CountShellMetric;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class PythonShellMetricsSpout extends ShellSpout implements IRichSpout {
	private static final long serialVersionUID = 1999209252187463355L;

	public PythonShellMetricsSpout(String[] command) {
		super(command);
	}

    public PythonShellMetricsSpout(String command, String file) {
        super(command, file);
    }

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);

		CountShellMetric cMetric = new CountShellMetric();
		context.registerMetric("my-custom-shellspout-metric", cMetric, 5);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("field1"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
