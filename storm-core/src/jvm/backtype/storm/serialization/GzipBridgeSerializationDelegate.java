package backtype.storm.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Always writes gzip out, but tests incoming to see if it's gzipped. If it is, deserializes with gzip. If not, uses
 * {@link backtype.storm.serialization.DefaultSerializationDelegate} to deserialize. Any logic needing to be enabled
 * via {@link #prepare(java.util.Map)} is passed through to both delegates.
 * @author danehammer
 */
public class GzipBridgeSerializationDelegate implements SerializationDelegate {

    private DefaultSerializationDelegate defaultDelegate = new DefaultSerializationDelegate();
    private GzipSerializationDelegate gzipDelegate = new GzipSerializationDelegate();

    @Override
    public void prepare(Map stormConf) {
        defaultDelegate.prepare(stormConf);
        gzipDelegate.prepare(stormConf);
    }

    @Override
    public byte[] serialize(Object object) {
        return gzipDelegate.serialize(object);
    }

    @Override
    public Object deserialize(byte[] bytes) {
        if (isGzipped(bytes)) {
            return gzipDelegate.deserialize(bytes);
        } else {
            return defaultDelegate.deserialize(bytes);
        }
    }

    /**
     * Looks ahead to see if the GZIP magic constant is heading the input stream
     */
    private boolean isGzipped(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        PushbackInputStream pis = new PushbackInputStream(bis, 2);

        byte[] header = new byte[2];
        try {
            pis.read(header);
            // Push those two bytes back into the stream
            pis.unread(header);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Split up GZIP_MAGIC into readable bytes
        byte magicFirst = (byte) GZIPInputStream.GZIP_MAGIC;
        byte magicSecond =(byte) (GZIPInputStream.GZIP_MAGIC >> 8);

        return (header[0] == magicFirst) && (header[1] == magicSecond);
    }
}
