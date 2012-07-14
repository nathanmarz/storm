package backtype.storm.testing;

import backtype.storm.Config;

public class CompleteTopologyParam {
	private MockedSources mockedSources;
	private Config stormConf;
	private Boolean cleanupState;
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
