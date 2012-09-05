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
