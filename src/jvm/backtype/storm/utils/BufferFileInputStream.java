package backtype.storm.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;


public class BufferFileInputStream {
    byte[] buffer;
    FileInputStream stream;

    public BufferFileInputStream(String file, int bufferSize) throws FileNotFoundException {
        stream = new FileInputStream(file);
        buffer = new byte[bufferSize];
    }

    public BufferFileInputStream(String file) throws FileNotFoundException {
        this(file, 15*1024);
    }

    public byte[] read() throws IOException {
        int length = stream.read(buffer);
        if(length==-1) {
            close();
            return new byte[0];
        } else if(length==buffer.length) {
            return buffer;
        } else {
            return Arrays.copyOf(buffer, length);
        }
    }

    public void close() throws IOException {
        stream.close();
    }
}
