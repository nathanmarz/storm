package backtype.storm.nimbus;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * @author slukjanov
 */
public interface INimbusStorage {

    void init(Map conf);

    InputStream open(String path);

    OutputStream create(String path);

    List<String> list(String path);

    List<String> list(String path, boolean fullPath);

    void delete(String path);

    void delete(List<String> path);

    void mkdirs(String path);

    void move(String from, String to);

    boolean isSupportDistributed();

}

