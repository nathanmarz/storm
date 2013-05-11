package storm.kafka;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 11/05/2013
 * Time: 14:43
 */
public class StaticHosts implements BrokerHosts {


	public List<HostPort> hosts;
	public int partitionsPerHost;

	public static StaticHosts fromHostString(List<String> hostStrings, int partitionsPerHost) {
		return new StaticHosts(convertHosts(hostStrings), partitionsPerHost);
	}

	public StaticHosts(List<HostPort> hosts, int partitionsPerHost) {
		this.hosts = hosts;
		this.partitionsPerHost = partitionsPerHost;
	}

	public static List<HostPort> convertHosts(List<String> hosts) {
		List<HostPort> ret = new ArrayList<HostPort>();
		for (String s : hosts) {
			HostPort hp;
			String[] spec = s.split(":");
			if (spec.length == 1) {
				hp = new HostPort(spec[0]);
			} else if (spec.length == 2) {
				hp = new HostPort(spec[0], Integer.parseInt(spec[1]));
			} else {
				throw new IllegalArgumentException("Invalid host specification: " + s);
			}
			ret.add(hp);
		}
		return ret;
	}

}
