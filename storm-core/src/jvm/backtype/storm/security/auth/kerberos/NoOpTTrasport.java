package backtype.storm.security.auth.kerberos;

import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by pshah on 8/12/15.
 */
public class NoOpTTrasport extends TSaslServerTransport {

    public NoOpTTrasport(TTransport transport) {
        super(transport);
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void open() throws TTransportException {

    }

    @Override
    public void close() {

    }

    @Override
    public int read(byte[] bytes, int i, int i1) throws TTransportException {
        return 0;
    }

    @Override
    public void write(byte[] bytes, int i, int i1) throws TTransportException {

    }
}
