package backtype.storm.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * @author slukjanov
 */
public class BufferInputStream {

    private static final int DEFAULT_BUFFER_SIZE = 15 * 1024;

    private byte[] buffer;
    private InputStream delegate;

    public BufferInputStream(InputStream delegate, int bufferSize) {
        this.delegate = delegate;
        this.buffer = new byte[bufferSize];
    }

    public BufferInputStream(InputStream delegate) {
        this(delegate, DEFAULT_BUFFER_SIZE);
    }

    public byte[] read() throws IOException {
        int length = delegate.read(buffer);
        if (length == -1) {
            close();
            return new byte[0];
        } else if (length == buffer.length) {
            return buffer;
        } else {
            return Arrays.copyOf(buffer, length);
        }
    }

    public void close() throws IOException {
        delegate.close();
    }

}
