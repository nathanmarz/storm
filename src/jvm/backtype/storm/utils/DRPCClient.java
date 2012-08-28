package backtype.storm.utils;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DistributedRPC;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

public class DRPCClient implements DistributedRPC.Iface {
    private TTransport conn;
    private DistributedRPC.Client client;
    private String host;
    private int port;
    private Integer timeout;

    public DRPCClient(String host, int port, Integer timeout) {
        try {
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            connect();
        } catch(TException e) {
            throw new RuntimeException(e);
        }
    }
    
    public DRPCClient(String host, int port) {
        this(host, port, null);
    }
    
    private void connect() throws TException {
        TSocket socket = new TSocket(host, port);
        if(timeout!=null) {
            socket.setTimeout(timeout);
        }
        conn = new TFramedTransport(socket);
        client = new DistributedRPC.Client(new TBinaryProtocol(conn));
        conn.open();
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }   
    
    
    /**
     * Wraps the executeBinary method, turning the ByteBuffer into a String. Required for backwards compatibility.
     * @param func
     * @param args
     * @return
     * @throws TException
     * @throws DRPCExecutionException
     */
    public String execute(String func, String args) throws TException, DRPCExecutionException {
        try {
            ByteBuffer byteBuffer = executeBinary( func, args );
            Charset charset = Charset.defaultCharset();

            CharsetDecoder decoder = charset.newDecoder();  
            CharBuffer charBuffer = decoder.decode(byteBuffer);  
            String s = charBuffer.toString();           
            
            return s;
        } catch(TException e) {
            client = null;
            throw e;
        } catch( java.nio.charset.CharacterCodingException e ) {
            client = null;
            throw new DRPCExecutionException( "Error encoding characters: " + e.toString() );
        } catch(DRPCExecutionException e) {
            client = null;
            throw e;
        }
    }

    public ByteBuffer executeBinary(String func, String args) throws TException, DRPCExecutionException {
        try {
            if(client==null) connect();
            return client.executeBinary(func, args);
        } catch(TException e) {
            client = null;
            throw e;
        } catch(DRPCExecutionException e) {
            client = null;
            throw e;
        }
    }
    
    public void close() {
        conn.close();
    }
}
