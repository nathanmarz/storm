package backtype.storm.utils;

import backtype.storm.generated.Nimbus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;

import java.lang.Integer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WorkerHbProxy {
    public static final Log LOG = LogFactory.getLog(WorkerHbProxy.class);
    private TTransport transport = null;
    private Nimbus.Client client = null;
    private String host;
    private int port;
    private Map stormMap;

    public WorkerHbProxy(String host,int port, Map stormMap) {
        this.host = host;
        this.port = port;
        this.stormMap = stormMap;
    }

    private void init() throws TException {
        if (transport == null) {
            LOG.info("Connecting to Nimbus at " + host + ":" + port);
            transport = new TFramedTransport(new TSocket(host, port));
            client = new Nimbus.Client(new TBinaryProtocol(transport));
            transport.open();
        }
    }

    private void close() {
        if (transport != null) {
            transport.close();
            transport = null;
            client = null;
        }
    }

    public void workerHeartBeat(String stormId, String workerId, int port,
                                Set<List<Integer>> executors, long upTime, long HBTime,ByteBuffer buf) {
        LOG.info("do workerHeartBeat");
        try {
            init();
            client.workerHeartBeat(stormId, workerId, port, executors, upTime,
                    HBTime,buf);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            close();
        }
    }
}
