package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.security.auth.ThriftClient;
import backtype.storm.generated.Nimbus;

import java.util.Map;

public class NimbusClient extends ThriftClient {	
    private Nimbus.Client _client;
    
    public static NimbusClient getConfiguredClient(Map conf) {
	String nimbusHost = (String) conf.get(Config.NIMBUS_HOST);
	int nimbusPort = Utils.getInt(conf.get(Config.NIMBUS_THRIFT_PORT));
	return new NimbusClient(nimbusHost, nimbusPort);
    }
    
    public NimbusClient(String host) {
	this(host, 6627);
    }
    
    public NimbusClient(String host, int port) {
	super(host, port, "nimbus_server");
	_client = new Nimbus.Client(_protocol);
    }
    
    public Nimbus.Client getClient() {
	return _client;
    }
}
