package backtype.storm.testing;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import clojure.lang.Keyword;

public class MkClusterParam extends HashMap<Object, Object>{
	public MkClusterParam() {}
	
	public MkClusterParam(Map map) {
		super(map);
	}
	
	public Integer getSupervisors() {
		Object supervisors = get(Keyword.intern("supervisors"));
		return (supervisors == null) ? null : (Integer)supervisors;
	}
	
	public void setSupervisors(int supervisors) {
		put(Keyword.intern("supervisors"), supervisors);
	}
	
	public Integer getPortsPerSupervisor() {
		Object portsPerSupervisor = get(Keyword.intern("ports-per-supervisor"));
		return (portsPerSupervisor == null) ? null : (Integer)portsPerSupervisor;
	}
	
	public void setPortsPerSupervisor(int portsPerSupervisor) {
		put(Keyword.intern("ports-per-supervisor"), portsPerSupervisor);
	}
	
	public Config getDaemonConf() {
		Object daemonConf = get(Keyword.intern("daemon-conf"));
		return (daemonConf == null) ? null : (Config)daemonConf;
	}
	
	public void setDaemonConf(Config config) {
		put(Keyword.intern("daemon-conf"), config);
	}
}
