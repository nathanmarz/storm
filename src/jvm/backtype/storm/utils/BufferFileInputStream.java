package backtype.storm.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;


public class BufferFileInputStream extends BufferInputStream {

    public BufferFileInputStream(String file, int bufferSize) throws FileNotFoundException {
        super(new FileInputStream(file), bufferSize);
    }

    public BufferFileInputStream(String file) throws FileNotFoundException {
        super(new FileInputStream(file));
    }

}
