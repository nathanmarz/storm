package backtype.storm.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;

public class CRC32OutputStream extends OutputStream {
    private CRC32 hasher;
    
    public CRC32OutputStream() {
        hasher = new CRC32();
    }
    
    public long getValue() {
        return hasher.getValue();
    }

    @Override
    public void write(int i) throws IOException {
        hasher.update(i);
    }

    @Override
    public void write(byte[] bytes, int start, int end) throws IOException {
        hasher.update(bytes, start, end);
    }    
}
