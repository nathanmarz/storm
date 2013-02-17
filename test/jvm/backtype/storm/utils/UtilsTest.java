package backtype.storm.utils;

import org.junit.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.curator.CuratorZookeeperClient;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.retry.ExponentialBackoffRetry;

import backtype.storm.Config;

public class UtilsTest {

  @Test
  public void testNewCuratorUsesBoundedExponentialBackoff() {
    @SuppressWarnings("unchecked")
    Map<String,Object> conf = (Map<String,Object>)Utils.readDefaultConfig();

    // Ensure these two values are different.
    final int ArbitraryInterval = 24;
    final int ArbitraryRetries = 4;
    conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, ArbitraryInterval);
    conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, ArbitraryRetries);

    List<String> servers = new ArrayList<String>();
    servers.add("bogus_server");
    Object port = new Integer(42);
    CuratorFramework cf = Utils.newCurator(conf, servers, port);

    assertTrue(cf.getZookeeperClient().getRetryPolicy() 
            instanceof ExponentialBackoffRetry);

    ExponentialBackoffRetry retry =
        (ExponentialBackoffRetry)cf.getZookeeperClient().getRetryPolicy();
    assertEquals(retry.getBaseSleepTimeMs(), ArbitraryInterval);
    assertEquals(retry.getN(), ArbitraryRetries);
  }
}
