package backtype.storm.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DistributedRPC;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;

public class DRPCClient implements DistributedRPC.Iface {
    private TTransport conn;
    private DistributedRPC.Client client;
    private String host;
    private int port;    
    private ExecutorService executor;

    public DRPCClient(String host, int port) {
        try {
            this.host = host;
            this.port = port;
            connect();
        } catch(TException e) {
            throw new RuntimeException(e);
        }
    }
    
    public DRPCClient(String host, int port, int threadCount){
    		this(host, port);
    		this.executor = Executors.newFixedThreadPool(threadCount);
    }
    
    public DRPCClient(String host, int port, ExecutorService executor){
    		this(host, port);
    		this.executor = executor;
    }
    
    private void connect() throws TException {
        conn = new TFramedTransport(new TSocket(host, port));
        client = new DistributedRPC.Client(new TBinaryProtocol(conn));
        conn.open();
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }   
    
    public String execute(String func, String args) throws TException, DRPCExecutionException {
        try {
            if(client==null) connect();
            return client.execute(func, args);
        } catch(TException e) {
            client = null;
            throw e;
        } catch(DRPCExecutionException e) {
            client = null;
            throw e;
        }
    }
    
    public Future<String> executeAsync(final String func, final String args) throws TException, DRPCExecutionException {
    		Future<String> future = this.executor.submit(new Callable<String>(){

				@Override
				public String call() throws Exception {
					return execute(func, args);
				}
    			
    		});
    		return future;
    }

    public void close() {
        conn.close();
        this.executor.shutdown();
    }
}
