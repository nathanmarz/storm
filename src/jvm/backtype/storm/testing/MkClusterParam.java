package backtype.storm.testing;

import backtype.storm.Config;

/**
 * The param arg for <code>Testing.withSimulatedTimeCluster</code> and <code>Testing.withTrackedCluster</code>
 */
public class MkClusterParam {
	/**
	 * count of supervisors for the cluster.
	 */
	private Integer supervisors;
	/**
	 * count of port for each supervisor
	 */
	private Integer portsPerSupervisor;
	/**
	 * cluster config
	 */
	private Config daemonConf;
	
	public Integer getSupervisors() {
		return supervisors;
	}
	public void setSupervisors(Integer supervisors) {
		this.supervisors = supervisors;
	}
	public Integer getPortsPerSupervisor() {
		return portsPerSupervisor;
	}
	public void setPortsPerSupervisor(Integer portsPerSupervisor) {
		this.portsPerSupervisor = portsPerSupervisor;
	}
	public Config getDaemonConf() {
		return daemonConf;
	}
	public void setDaemonConf(Config daemonConf) {
		this.daemonConf = daemonConf;
	}
}
