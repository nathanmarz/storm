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
package backtype.storm.testing;

import backtype.storm.Config;

/**
 * The param class for the <code>Testing.completeTopology</code>.
 */
public class CompleteTopologyParam {
	/**
	 * The mocked spout sources
	 */
	private MockedSources mockedSources;
	/**
	 * the config for the topology when it was submitted to the cluster
	 */
	private Config stormConf;
	/**
	 * whether cleanup the state?
	 */
	private Boolean cleanupState;
	/**
	 * the topology name you want to submit to the cluster
	 */
	private String topologyName;
	
	public MockedSources getMockedSources() {
		return mockedSources;
	}
	public void setMockedSources(MockedSources mockedSources) {
		this.mockedSources = mockedSources;
	}
	public Config getStormConf() {
		return stormConf;
	}
	public void setStormConf(Config stormConf) {
		this.stormConf = stormConf;
	}
	public Boolean getCleanupState() {
		return cleanupState;
	}
	public void setCleanupState(Boolean cleanupState) {
		this.cleanupState = cleanupState;
	}
	public String getTopologyName() {
		return topologyName;
	}
	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}
}
